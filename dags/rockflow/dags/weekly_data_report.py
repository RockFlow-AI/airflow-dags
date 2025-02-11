from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "monday_tasks_report",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 31, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 1",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as monday_tasks_report:
    this_month_data_report_task = SimpleHttpOperator(
        task_id="this_month_data_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/this-month-data-report",
        response_check=lambda response: response.json()["code"] == 200,
    )

    high_commission_report_task = SimpleHttpOperator(
        task_id="high_commission_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/high-commission-report",
        response_check=lambda response: response.json()["code"] == 200,
    )

    high_assets_report_task = SimpleHttpOperator(
        task_id="high_assets_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/high-assets-report",
        response_check=lambda response: response.json()["code"] == 200,
    )
