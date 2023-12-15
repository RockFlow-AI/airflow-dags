from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

# DAG for Monday to Friday
feed_news_scraping_weekdays = DAG(
    "feed_news_scraping_weekdays",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='0 10,13,16,18,21 * * 1-5',  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

# DAG for Saturday and Sunday
feed_news_scraping_weekends = DAG(
    "feed_news_scraping_weekends",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='0 15 * * 6,0',  # Cron expression for 3 PM on Saturday and Sunday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

# Task for weekdays
task_weekdays = SimpleHttpOperator(
    task_id='feed_news_scraping_weekdays',
    method='POST',
    http_conn_id='rockbot',
    endpoint='/bot/api/ideas/feed/news',
    response_check=lambda response: response.json()['code'] == 200,
    dag=feed_news_scraping_weekdays
)

# Task for weekends
task_weekends = SimpleHttpOperator(
    task_id='feed_news_scraping_weekends',
    method='POST',
    http_conn_id='rockbot',
    endpoint='/bot/api/ideas/feed/news',
    response_check=lambda response: response.json()['code'] == 200,
    dag=feed_news_scraping_weekends
)
