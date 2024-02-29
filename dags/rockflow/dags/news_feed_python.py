from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models.baseoperator import chain
# scrap DAG for Monday to Friday
with DAG(
    "feed_news_scraping_weekdays",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1, tz='Asia/Shanghai'),
    schedule_interval='0 0,9,13,15,17,20,22,23 * * 1-5',  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as feed_news_scraping_weekdays:
    # scrap task for weekdays
    scraping_task_weekdays = SimpleHttpOperator(
        task_id='feed_news_scraping_weekdays',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/scrap',
        response_check=lambda response: response.json()['code'] == 200,
        dag=feed_news_scraping_weekdays
    )
# scrap DAG for Saturday and Sunday
with DAG(
    "feed_news_scraping_weekends",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1, tz='Asia/Shanghai'),
    schedule_interval='0 15 * * 6,0',  # Cron expression for 3 PM on Saturday and Sunday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as feed_news_scraping_weekends:
    # Task for weekends
    scraping_task_weekends = SimpleHttpOperator(
        task_id='feed_news_scraping_weekends',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/scrap',
        response_check=lambda response: response.json()['code'] == 200,
        dag=feed_news_scraping_weekends
    )
# test send DAG for Monday to Friday
with DAG(
    "feed_news_inspiration_message_test",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='*/10 * * * *',  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as feed_news_inspiration_message_test:
    task_feed_news_inspiration_message_test = SimpleHttpOperator(
        task_id='feed_news_inspiration_message_test',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/test_send',
        response_check=lambda response: response.json()['code'] == 200,
    )
#  DAG for every day
with DAG(
    "feed_news_analyze_and_generate",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='*/10 * * * *',  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as feed_news_analyze_and_generate:
    # Task for analyze
    analyze_task = SimpleHttpOperator(
        task_id='feed_news_analyze',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/analyze',
        response_check=lambda response: response.json()['code'] == 200,
    )
    # Task for generate
    generate_task = SimpleHttpOperator(
        task_id='feed_news_generate',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/generate',
        response_check=lambda response: response.json()['code'] == 200,
    )
    analyze_task >> generate_task
# upload news DAG for Monday to Friday
with DAG(
    "feed_news_uploading",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * 1-5',  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as feed_news_uploading_weekdays:
    # scrap task for weekdays
    uploading_task = SimpleHttpOperator(
        task_id='feed_news_uploading',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/ideas/feed/news/upload_today_news',
        response_check=lambda response: response.json()['code'] == 200,
        dag=feed_news_uploading_weekdays
    )

with DAG(
    "feed_news_check_tradegpt_status",
    catchup=False,
    start_date=pendulum.datetime(2024, 2, 29, tz="Asia/Shanghai"),
    schedule_interval="0 11,14,17,19,22 * * *",  # Cron expression for specific times on Monday to Friday
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    },
) as feed_news_check_tradegpt_status:
    check_tradegpt_status = SimpleHttpOperator(
        task_id="feed_news_check_tradegpt_status",
        method="POST",
        http_conn_id="rockbot",
        endpoint="/bot/api/ideas/feed/news/acheck_tradegpt_status",
        response_check=lambda response: response.json()["code"] == 200,
        dag=feed_news_check_tradegpt_status,
    )
