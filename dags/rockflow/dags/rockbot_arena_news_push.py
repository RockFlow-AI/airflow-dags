from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "rockbot_arena_news_push",
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 23),
    schedule_interval='0 18-23/2,0-6/2 * * *',
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
        endpoint='/bot/api/arena/news_push',
        response_check=lambda response: response.json()['code'] == 200,
    )


