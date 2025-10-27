from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "rockbot_arena_trade",
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 23),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as rockbot_arena_trade:
    # arena trade
    rockbot_arena_trade_task = SimpleHttpOperator(
        task_id='rockbot_arena_trade',
        method='GET',
        http_conn_id='rockbot',
        endpoint='/bot/api/arena/schedule_routine',
        response_check=lambda response: response.json()['code'] == 200,
    )