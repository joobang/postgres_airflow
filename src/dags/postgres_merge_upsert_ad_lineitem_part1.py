from __future__ import annotations

from datetime import datetime
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator

"""
airflow Config 설정
weight_rule : 작업간의 우선순위 결정.
    - downstream : 후순위의 task일수록 우선순위가 낮아진다.
    - upstream : 마지막에 가까운 task일수록 높은 우선순위.
    - absolute : task가 모두 공평한 우선순위 할당.
owner : 소유자 이름
depends_on_past : True로 설정하면, 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스가 실패한 경우 실행되지 않고 대기.
retry_delay : 작업 실패 시 재시도 간격 설정값.
retries : 작업 실패 시 최대 재시도 횟수.
start_date : DAG 시작 날짜 설정.
max_active_tis_per_dag : 하나의 DAG에서 동시에 실행할 수 있는 최대 작업 인스턴스 개수 설정.
"""
dag_default_args: Dict[str, Any] = {
    **DAG_DEFAULT_ARGS,
    'start_date': datetime(2022, 9, 1),
    'max_active_tis_per_dag': 2,
}

#start_date 이전 데이터를 적재하기 위한 함수.
#첫 시행일 때만 first_merge_upsert task를 실행하고 나머지는 normal_merge_upsert 실행.
def is_first_date(**kwargs):
    start_date = kwargs['dag'].default_args['start_date']
    data_interval_start = kwargs['data_interval_start']
    print("print : ",data_interval_start, start_date)
    if start_date == data_interval_start:
        return 'first_merge_upsert'
    else:
        return 'normal_merge_upsert'

with DAG(
    'postgres_merge_upsert_ad_lineitem_part1',
    default_args=dag_default_args,
    description='This DAG merge-upserts the `ad_lineitem` table in the analytics_db with CDC logs from the source table',
    schedule_interval='0 * * * *',
) as dag:

    table = 'ad_lineitem'
    # normal_merge_upsert는 일반적인 upsert 작업으로 schedule_interval 만큼의 데이터를 적재한다.
    normal_merge_upsert = PostgresOperator(
        task_id='normal_merge_upsert',
        sql="""
            DELETE FROM
                {{ params.table }}
            USING
                {{ params.table }}_staging
            WHERE
                {{ params.table }}.id = {{ params.table }}_staging.id
                AND {{ params.table }}_staging.updated_at >= TIMESTAMP'{{ data_interval_start }}'
                AND {{ params.table }}_staging.updated_at < TIMESTAMP'{{ data_interval_end }}'
            ;

            INSERT INTO {{ params.table }}
            (
                SELECT DISTINCT ON (id)
                    id,
                    revenue_type,
                    created_at,
                    updated_at
                FROM
                    {{ params.table }}_staging
                WHERE 
                    updated_at >= TIMESTAMP'{{ data_interval_start }}'
                    AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                ORDER BY id, updated_at
            )
            ;
            
            INSERT INTO {{ params.table }}_scheduler
            (
                SELECT 
                    (SELECT COUNT(DISTINCT id)
                        FROM {{ params.table }}_staging
                        WHERE
                            updated_at >= TIMESTAMP'{{ data_interval_start }}'
                            AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                        ORDER BY id, updated_at) as staging_count,
                    (SELECT COUNT(*)
                        FROM {{ params.table }}
                        WHERE
                            updated_at >= TIMESTAMP'{{ data_interval_start }}'
                            AND updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
                    TIMESTAMP'{{ data_interval_start }}' as started_at,
                    TIMESTAMP'{{ data_interval_end }}' as end_at
            )
            ;

            DELETE FROM
            {{ params.table }}_staging
            WHERE 
                updated_at >= TIMESTAMP'{{ data_interval_start }}'
                AND updated_at < TIMESTAMP'{{ data_interval_end }}'
            ;
        """,
        params={
            'table': table
        },
        postgres_conn_id="analytics_db",
    )
    # start_date 이전의 데이터를 적재하기 위한 task로 첫 시행일때만 data_interval_end 이전 데이터를 적재한다.
    first_merge_upsert = PostgresOperator(
        task_id='first_merge_upsert',
        sql="""
            DELETE FROM
                {{ params.table }}
            USING
                {{ params.table }}_staging
            WHERE
                {{ params.table }}.id = {{ params.table }}_staging.id
                AND {{ params.table }}_staging.updated_at < TIMESTAMP'{{ data_interval_end }}'
            ;

            INSERT INTO {{ params.table }}
            (
                SELECT DISTINCT ON (id)
                    id,
                    revenue_type,
                    created_at,
                    updated_at
                FROM
                    {{ params.table }}_staging
                WHERE 
                    updated_at < TIMESTAMP'{{ data_interval_end }}'
                ORDER BY id, updated_at
            )
            ;
            
            INSERT INTO {{ params.table }}_scheduler
            (
                SELECT 
                    (SELECT COUNT(DISTINCT id)
                        FROM {{ params.table }}_staging
                        WHERE
                            updated_at < TIMESTAMP'{{ data_interval_end }}'
                        ORDER BY id, updated_at) as staging_count,
                    (SELECT COUNT(*)
                        FROM {{ params.table }}
                        WHERE
                            updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
                    TIMESTAMP'{{ data_interval_start }}' as started_at,
                    TIMESTAMP'{{ data_interval_end }}' as end_at
            )
            ;

            DELETE FROM
            {{ params.table }}_staging
            WHERE 
                updated_at < TIMESTAMP'{{ data_interval_end }}'
            ;
        """,
        params={
            'table': table
        },
        postgres_conn_id="analytics_db",
    )
    # 첫 시행인지 구분하기위한 task
    task_branch = BranchPythonOperator(
        task_id='task_branch',
        python_callable=is_first_date,
        provide_context=True,
        dag=dag
    )
    
    task_branch >> [first_merge_upsert,normal_merge_upsert]