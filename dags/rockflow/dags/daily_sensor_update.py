from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "daily_sensor_update",
    catchup=False,
    start_date=pendulum.datetime(2025, 11, 8, tz="Asia/Shanghai"),
    schedule_interval="0 10 * * *",
    default_args={
        "owner": "xuwenzhe",
        "depends_on_past": False,
        "retries": 0,
    },
) as daily_sensor_update:
    daily_sensor_update_task = SimpleHttpOperator(
        task_id="daily_sensor_update_process",
        method="POST",
        http_conn_id="tradebot",
        endpoint="/bot/api/sensor_update",
        response_check=lambda response: response.json().get("code") == 200,
    )
