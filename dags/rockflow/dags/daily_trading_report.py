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
    start_date=pendulum.datetime(2024, 12, 4, tz="Asia/Shanghai"),
    schedule_interval="0 9 * * 2-6",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as daily_trading_report:
    daily_trading_report_task = SimpleHttpOperator(
        task_id="this_month_data_report",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/data/this-month-data-report",
        response_check=lambda response: response.json()["code"] == 200,
    )
