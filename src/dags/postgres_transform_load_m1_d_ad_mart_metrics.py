from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict
from utils.dag import DAG_DEFAULT_ARGS

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.time_delta import TimeDeltaSensor


dag_default_args: Dict[str, Any] = {
    **DAG_DEFAULT_ARGS,
    'start_date': datetime(2022, 9, 1),
    'max_active_tis_per_dag': 10,
}

with DAG(
    'postgres_transform_load_m1_d_ad_mart_metrics_original',
    default_args=dag_default_args,
    description='This DAG builds `m1_d_ad_mart_metrics` mart table.',
    schedule_interval='0 15 * * *',
) as dag:

    table = 'm1_d_ad_unit_metrics'

    wait_for_logs = TimeDeltaSensor(
        task_id='wait_for_logs',
        mode='reschedule',
        timeout=1800,
        poke_interval=5,
        delta=timedelta(seconds=3600),
    )

    transform_load = PostgresOperator(
        task_id='transform_load',
        sql="""
            INSERT INTO {{ params.table }}
            (
                SELECT
                    aggs.lineitem_id,
                    l.revenue_type,
                    u.unit_type,
                    aggs.unit_id,
                    aggs.data_at,
                    aggs.impression_count,
                    aggs.click_count,
                    aggs.revenue_sum
                FROM
                (
                    SELECT
                        DATE_TRUNC('hour', created_at) AS data_at,
                        lineitem_id,
                        unit_id,
                        COUNT(*)                       AS impression_count,
                        0                              AS click_count,
                        SUM(revenue)                   AS revenue_sum
                    FROM
                        ad_impression
                    WHERE
                        created_at >= TIMESTAMP'{{ data_interval_start }}'
                        AND created_at < TIMESTAMP'{{ data_interval_end }}'
                    GROUP BY
                        1, 2, 3

                    UNION ALL

                    SELECT
                        DATE_TRUNC('hour', created_at) AS data_at,
                        lineitem_id,
                        unit_id,
                        0                              AS impression_count,
                        COUNT(*)                       AS click_count,
                        SUM(revenue)                   AS revenue_sum
                    FROM
                        ad_click
                    WHERE
                        created_at >= TIMESTAMP'{{ data_interval_start }}'
                        AND created_at < TIMESTAMP'{{ data_interval_end }}'
                    GROUP BY
                        1, 2, 3
                ) aggs
                    INNER JOIN ad_lineitem l ON
                        aggs.lineitem_id = l.id
                    INNER JOIN unit u ON
                        aggs.unit_id = u.id
            )
            ;
        """,
        params={'table': table},
        postgres_conn_id="analytics_db",
    )

    wait_for_logs >> transform_load
