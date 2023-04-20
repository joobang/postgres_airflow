from __future__ import annotations

from datetime import datetime
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

with DAG(
    'postgres_merge_upsert_ad_lineitem_test',
    default_args=dag_default_args,
    description='This DAG merge-upserts the `ad_lineitem` table in the analytics_db with CDC logs from the source table',
    schedule_interval='0 * * * *',
) as dag:

    table = 'ad_lineitem'

    merge_upsert = PostgresOperator(
        task_id='merge_upsert',
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
                            AND updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
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
