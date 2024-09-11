from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "rockbot_refresh_config",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as rockbot_refresh_config:
    # refresh config
    rockbot_refresh_config_task = SimpleHttpOperator(
        task_id='rockbot_refresh_config',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/config/refresh_config',
        response_check=lambda response: response.json()['code'] == 200,
    )

with DAG(
    "rockbot_clean_expired_graphs",
    catchup=False,
    start_date=pendulum.datetime(2024, 9, 10),
    schedule_interval="*/60 * * * *",
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as rockbot_clean_expired_graphs:
    # rockbot_clean_expired_graphs
    rockbot_clean_expired_graphs_task = SimpleHttpOperator(
        task_id="rockbot_clean_expired_graphs",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/graphs/clean_expired_graphs",
        response_check=lambda response: response.json()["code"] == 200,
    )
