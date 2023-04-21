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
    schedule_interval='0 * * * *',
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
                AND {{ params.table }}_staging.updated_at >= TIMESTAMP'{{ data_interval_start }}'
                AND {{ params.table }}_staging.updated_at < TIMESTAMP'{{ data_interval_end }}'
            ;

            INSERT INTO {{ params.table }}
            (
                SELECT DISTINCT ON (id)
                    id,
                    unit_type,
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
