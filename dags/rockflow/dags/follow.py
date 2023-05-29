import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

default_follow_stat = DAG(
    "default_follow_stat",
    catchup=False,
    start_date=pendulum.datetime(2023, 5, 29, tz='UTC'),
    schedule_interval='0 7 * * 1',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='default_follow_stat',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/user/follow/statDefaultFollow/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=default_follow_stat,
)