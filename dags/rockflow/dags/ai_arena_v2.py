from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "rockbot_arena_trade_v2",
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 23),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as rockbot_arena_trade_v2:
    # arena trade v2
    rockbot_arena_trade_v2_task = SimpleHttpOperator(
        task_id='rockbot_arena_trade_v2',
        method='GET',
        http_conn_id='rockbot',
        endpoint='/bot/api/arena/v2/ai_arena_v2',
        response_check=lambda response: response.json()['code'] == 200,
    )


