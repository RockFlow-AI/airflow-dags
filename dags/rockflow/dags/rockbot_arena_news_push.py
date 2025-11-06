from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "rockbot_arena_news_push",
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 23, tz='Asia/Shanghai'),
    schedule_interval='0 17-23,0-10 * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as rockbot_arena_news_push:
    # arena news push
    rockbot_arena_news_push_task = SimpleHttpOperator(
        task_id='rockbot_arena_news_push',
        method='GET',
        http_conn_id='rockbot',
        endpoint='/bot/api/arena/v2/news_push_v2',
        response_check=lambda response: response.json()['code'] == 200,
    )


