from datetime import datetime, timedelta
from airflow import DAG
import pendulum
import json
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    "support_daily_report",
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 26, tz="Asia/Shanghai"),
    schedule_interval="0 10 * * *",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as support_daily_report:
    support_daily_report_task = SimpleHttpOperator(
        task_id="support_daily_report",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({}),
        http_conn_id="rockbot",
        endpoint="/inner/support/conversation-report",
        response_check=lambda response: response.json()["code"] == 200,
    )
