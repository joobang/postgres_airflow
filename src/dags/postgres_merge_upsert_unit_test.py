from __future__ import annotations

from datetime import datetime
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag_default_args: Dict[str, Any] = {
    **DAG_DEFAULT_ARGS,
    'start_date': datetime(2022, 9, 1),
    'max_active_tis_per_dag': 2,
}

with DAG(
    'postgres_merge_upsert_unit_test',
    default_args=dag_default_args,
    description='This DAG that merge-upserts the `units` table in the analytics_db with CDC logs from the source table',
    schedule_interval='@once',
    max_active_runs=1,
) as dag:

    table = 'unit'

    merge_upsert = PostgresOperator(
        task_id='merge_upsert',
        sql="""
            DELETE FROM
                {{ params.table }}
            USING
                {{ params.table }}_staging
            WHERE
                {{ params.table }}.id = {{ params.table }}_staging.id
                AND {{ params.table }}.updated_at >= TIMESTAMP'{{ params.start_time }}'
                AND {{ params.table }}.updated_at < TIMESTAMP'{{ params.end_time }}'
            ;

            INSERT INTO {{ params.table }}
            (
                SELECT
                    id,
                    unit_type
                FROM
                    {{ params.table }}_staging
                WHERE 
                    updated_at >= TIMESTAMP'{{ params.start_time }}'
                    AND updated_at < TIMESTAMP'{{ params.end_time }}'
                ORDER BY id, updated_at
            )
            ;

           DELETE FROM
            {{ params.table }}_staging
            WHERE 
                updated_at >= TIMESTAMP'{{ params.start_time }}'
                AND updated_at < TIMESTAMP'{{ params.end_time }}'
            ;
        """,
        params={
            'table': table,
            'start_time': '2022-09-01 00:00:00',
            'end_time': '2022-09-01 01:00:00'
        },
        postgres_conn_id="analytics_db",
    )
