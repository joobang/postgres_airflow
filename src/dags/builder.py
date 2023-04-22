from __future__ import annotations
import os
import yaml

from datetime import datetime
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator

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

config_path = './configs/postgres_merge_upsert'
 
for merge_upsert in os.listdir(config_path):
    if merge_upsert.endswith('.yaml'):
        file_path = os.path.join(config_path, merge_upsert)
        yaml_data = load_yaml(file_path)
        print(yaml_data)
        
        dag_default_args: Dict[str, Any] = {
            **DAG_DEFAULT_ARGS,
            'start_date': yaml_data['pipeline_dag_configs']['start_date'],
            'max_active_tis_per_dag': 2,
        }
        if 'pipeline_key' in yaml_data:
            dag_id = yaml_data['pipeline_type']+'_'+yaml_data['pipeline_key']
        else:
            dag_id = yaml_data['pipeline_type']
        with DAG(
            dag_id,
            default_args=dag_default_args,
            description='This DAG merge-upserts '+yaml_data['pipeline_key'],
            schedule_interval=yaml_data['pipeline_dag_configs']['schedule_interval']
        ) as dag:
            merge_upsert = yaml_data['merge_upsert']
            destination_table = merge_upsert['destination_table']
            staging_table = merge_upsert['staging_table']
            unique_keys = merge_upsert['unique_keys']
            keys = ','.join(unique_keys)
            
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
                        SELECT DISTINCT ON ({{ params.keys }})
                            *
                        FROM
                            {{ params.staging_table }}
                        WHERE 
                            updated_at >= TIMESTAMP'{{ data_interval_start }}'
                            AND updated_at < TIMESTAMP'{{ data_interval_end }}'
                        ORDER BY id, updated_at
                    )
                    ;

                    INSERT INTO {{ params.destination_table }}_scheduler
                    (
                        SELECT 
                            (SELECT COUNT(*)
                                FROM (
                                    SELECT {{ params.keys }}
                                    FROM {{ params.staging_table }}
                                    GROUP BY {{ params.keys }}
                                )
                                WHERE
                                    updated_at >= TIMESTAMP'{{ data_interval_start }}'
                                    AND updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
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
                    'keys':keys
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
                        AND {{ params.table }}_staging.updated_at < TIMESTAMP'{{ data_interval_end }}'
                    ;

                    INSERT INTO {{ params.destination_table }}
                    (
                        SELECT DISTINCT ON (id)
                            *
                        FROM
                            {{ params.staging_table }}
                        WHERE 
                            updated_at < TIMESTAMP'{{ data_interval_end }}'
                        ORDER BY id, updated_at
                    )
                    ;

                    INSERT INTO {{ params.destination_table }}_scheduler
                    (
                        SELECT 
                            (SELECT COUNT(*)
                                FROM (
                                    SELECT {{ params.keys }}
                                    FROM {{ params.staging_table }}
                                    GROUP BY {{ params.keys }}
                                )
                                WHERE
                                    updated_at < TIMESTAMP'{{ data_interval_end }}') as staging_count,
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
                    'keys':keys
                },
                postgres_conn_id=yaml_data['postgres']['conn_id'],
            )
            task_branch = BranchPythonOperator(
                task_id='task_branch',
                python_callable=is_first_date,
                provide_context=True,
                dag=dag
            )
            task_branch >> [first_merge_upsert,normal_merge_upsert]