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
    task_id=' ',
    method='DELETE',
    http_conn_id='flow-news',
    endpoint='/news/inner/users/feeds',
    response_check=lambda response: response.json()['code'] == 200,
    dag=user_feed_clear,
)