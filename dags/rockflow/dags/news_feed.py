from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


user_feed_clear = DAG(
    "user_feed_clear",
    catchup=False,
    start_date=pendulum.datetime(2023, 12, 13),
    schedule_interval='0 * * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='user_feed_clear',
    method='DELETE',
    http_conn_id='flow-news',
    endpoint='/news/inner/users/feeds',
    response_check=lambda response: response.json()['code'] == 200,
    dag=user_feed_clear,
)


news_feed_virtual_like_task = DAG(
    "news_feed_virtual_like",
    catchup=False,
    start_date=pendulum.datetime(2023, 12, 19, tz='America/New_York'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='news_feed_virtual_like',
    method='PATCH',
    http_conn_id='flow-news',
    endpoint='/news/inner/feeds',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=news_feed_virtual_like_task,
)
