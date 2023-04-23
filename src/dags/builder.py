from __future__ import annotations
import os
import yaml

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.external_task import ExternalTaskSensor

"""
is_first_date : start_date 이전 데이터를 적재하기 위한 함수.
                첫 시행일 때만 first_merge_upsert task를 실행하고 나머지는 normal_merge_upsert 실행.

load_yaml : yaml 파일 읽어오는 함수.

dag_template : yaml 파일을 읽어서 dag를 등록하는 함수.

"""
def is_first_date(**kwargs):
    start_date = kwargs['dag'].default_args['start_date']
    data_interval_start = kwargs['data_interval_start']
    print("print : ",data_interval_start, start_date)
    if start_date == data_interval_start:
        return 'first_merge_upsert'
    else:
        return 'normal_merge_upsert'

def load_yaml(file_path: str):
    with open(file_path, 'r') as file:
        yaml_data = yaml.safe_load(file)
    return yaml_data

def dag_template(yaml_data):
    dag_default_args: Dict[str, Any] = {
        **yaml_data['pipeline_dag_configs']['default_args'],
        'start_date': yaml_data['pipeline_dag_configs']['start_date'],
        'max_active_tis_per_dag': 2,
    }
        
    if 'end_date' in yaml_data['pipeline_dag_configs']:
        dag_default_args['end_date'] = yaml_data['pipeline_dag_configs']['end_date']
        
    if 'retry_delay' in yaml_data['pipeline_dag_configs']['default_args']:
        yaml_data['pipeline_dag_configs']['default_args']['retry_delay'] = timedelta(minutes=yaml_data['pipeline_dag_configs']['default_args']['retry_delay'])
        
    if 'pipeline_key' in yaml_data:
        dag_id = yaml_data['pipeline_type']+'_'+yaml_data['pipeline_key']
    else:
        dag_id = yaml_data['pipeline_type']
    
    merge_upsert = yaml_data['merge_upsert']
    destination_table = merge_upsert['destination_table']
    staging_table = merge_upsert['staging_table']
    unique_keys = ','.join(merge_upsert['unique_keys'])    
    
    @dag(dag_id=dag_id, default_args=dag_default_args,description='This DAG merge-upserts '+yaml_data['pipeline_key'],schedule_interval=yaml_data['pipeline_dag_configs']['schedule_interval'])    
    def yaml_dag():
        normal_merge_upsert = PostgresOperator(
            task_id='normal_merge_upsert',
            sql="""
                DELETE FROM
                    {{ params.destination_table }}
                USING
                    {{ params.staging_table }}
                WHERE
                    {{ params.destination_table }}.id = {{ params.staging_table }}.id
                    AND {{ params.staging_table }}.updated_at >= TIMESTAMP'{{ data_interval_start }}'
                    AND {{ params.staging_table }}.updated_at < TIMESTAMP'{{ data_interval_end }}'
                ;

                INSERT INTO {{ params.destination_table }}
                (
                    SELECT DISTINCT ON ({{ params.unique_keys }})
                        *
                    FROM
                        {{ params.staging_table }}
                    WHERE 
                        updated_at >= TIMESTAMP'{{ data_interval_start }}'
                        AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                    ORDER BY {{ params.unique_keys }}, updated_at
                )
                ;

                INSERT INTO {{ params.destination_table }}_scheduler
                (
                    SELECT 
                        (SELECT COUNT(*)
                            FROM (
                                SELECT DISTINCT ON ({{ params.unique_keys }})
                                    *
                                FROM {{ params.staging_table }}
                                WHERE
                                    updated_at >= TIMESTAMP'{{ data_interval_start }}'
                                    AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                                ORDER BY {{ params.unique_keys }}, updated_at
                            ) t ) as staging_count,
                        (SELECT COUNT(*)
                            FROM {{ params.destination_table }}
                            WHERE
                                updated_at >= TIMESTAMP'{{ data_interval_start }}'
                                AND updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
                        TIMESTAMP'{{ data_interval_start }}' as started_at,
                        TIMESTAMP'{{ data_interval_end }}' as end_at
                )
                ;

                DELETE FROM
                    {{ params.staging_table }}
                WHERE 
                    updated_at >= TIMESTAMP'{{ data_interval_start }}'
                    AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                ;
            """,
            params={
                'destination_table': destination_table,
                'staging_table': staging_table,
                'unique_keys':unique_keys
            },
            postgres_conn_id=yaml_data['postgres']['conn_id'],
        )

        first_merge_upsert = PostgresOperator(
            task_id='first_merge_upsert',
            sql="""
                DELETE FROM
                    {{ params.destination_table }}
                USING
                    {{ params.staging_table }}
                WHERE
                    {{ params.destination_table }}.id = {{ params.staging_table }}.id
                    AND {{ params.staging_table }}.updated_at < TIMESTAMP'{{ data_interval_end }}'
                ;

                INSERT INTO {{ params.destination_table }}
                (
                    SELECT DISTINCT ON ({{ params.unique_keys }})
                        *
                    FROM
                        {{ params.staging_table }}
                    WHERE 
                        updated_at < TIMESTAMP'{{ data_interval_end }}'
                    ORDER BY {{ params.unique_keys }}, updated_at
                )
                ;

                INSERT INTO {{ params.destination_table }}_scheduler
                (
                    SELECT 
                        (SELECT COUNT(*)
                            FROM (
                                SELECT DISTINCT ON ({{ params.unique_keys }})
                                    *
                                FROM {{ params.staging_table }}
                                WHERE
                                    updated_at < TIMESTAMP'{{ data_interval_end }}'
                                ORDER BY {{ params.unique_keys }}, updated_at
                            ) t ) as staging_count,
                        (SELECT COUNT(*)
                            FROM {{ params.destination_table }}
                            WHERE
                                updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
                        TIMESTAMP'{{ data_interval_start }}' as started_at,
                        TIMESTAMP'{{ data_interval_end }}' as end_at
                )
                ;

                DELETE FROM
                    {{ params.staging_table }}
                WHERE 
                    updated_at < TIMESTAMP'{{ data_interval_end }}'
                ;
            """,
            params={
                'destination_table': destination_table,
                'staging_table': staging_table,
                'unique_keys':unique_keys
            },
            postgres_conn_id=yaml_data['postgres']['conn_id'],
        )
        
        task_branch = BranchPythonOperator(
            task_id='task_branch',
            python_callable=is_first_date,
            provide_context=True
        )
        
        if 'execution_delay_seconds' in yaml_data:
            wait_for_logs = TimeDeltaSensor(
                    task_id='wait_for_logs',
                    mode='reschedule',
                    timeout=1800,
                    poke_interval=5,
                    delta=timedelta(seconds=yaml_data['execution_delay_seconds']),
            )
            if 'upstream_dependencies' in yaml_data:
                last_task = None
                for upstream in yaml_data['upstream_dependencies']:
                    updag = ExternalTaskSensor(
                        task_id='wait_for_'+upstream['dag_id'],
                        external_dag_id=upstream['dag_id'],
                        execution_delta=timedelta(seconds=upstream.get('timedelta_seconds',0)),
                        poke_interval=upstream.get('poke_interval',180),
                        timeout=upstream.get('timeout',7200),
                    )
                    if last_task:
                        last_task >> updag
                    last_task = updag
                
                last_task >> wait_for_logs >> task_branch >> [first_merge_upsert,normal_merge_upsert] 
            else:
                wait_for_logs >> task_branch >> [first_merge_upsert,normal_merge_upsert]  
        else:
            task_branch >> [first_merge_upsert,normal_merge_upsert]  
    
    return yaml_dag()

file_dir = os.path.dirname(os.path.abspath(__file__+'/../'))

for merge_upsert in os.listdir(file_dir+'/configs/postgres_merge_upsert'):
    if merge_upsert.endswith('.yaml'):
        file_path = os.path.join(file_dir+'/configs/postgres_merge_upsert/', merge_upsert)
        yaml_data = load_yaml(file_path)
        
        globals()[yaml_data['pipeline_type']+'_'+yaml_data['pipeline_key']] = dag_template(yaml_data)