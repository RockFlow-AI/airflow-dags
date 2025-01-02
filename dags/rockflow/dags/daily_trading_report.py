from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    "daily_trading_report",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 4, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 2-6",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as daily_trading_report:
    daily_trading_report_task = SimpleHttpOperator(
        task_id="daily_trading_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/daily-trading-report",
        response_check=lambda response: response.json()["code"] == 200,
    )

with DAG(
    "this_month_data_report",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 31, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 2-6",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as this_month_data_report:
    this_month_data_report_task = SimpleHttpOperator(
        task_id="this_month_data_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/this-month-data-report",
        response_check=lambda response: response.json()["code"] == 200,
    )

with DAG(
    "high_commission_report",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 6, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 1",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as high_commission_report:
    high_commission_report_task = SimpleHttpOperator(
        task_id="high_commission_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/high-commission-report",
        response_check=lambda response: response.json()["code"] == 200,
    )
    
with DAG(
    "high_assets_report",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 6, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 1",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as high_assets_report:
    high_assets_report_task = SimpleHttpOperator(
        task_id="high_assets_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/high-assets-report",
        response_check=lambda response: response.json()["code"] == 200,
    )
