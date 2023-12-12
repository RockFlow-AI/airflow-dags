from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


feed_news_scraping = DAG(
    "feed_news_scraping",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='0 9-21/2 * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='feed_news_scraping',
    method='POST',
    http_conn_id='rockbot',
    endpoint='/bot/api/ideas/feed/news',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=feed_news_scraping,
)