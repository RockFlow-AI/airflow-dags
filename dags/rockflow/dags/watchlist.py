import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import timedelta

WATCHLIST_US_DAY = DAG(
    "WATCHLIST_US_DAY",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='0 17 * * 1-5',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='WATCHLIST_US_DAY',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tasks',
    data={"period": "DAY"},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=WATCHLIST_US_DAY,
)

WATCHLIST_US_MINUTE = DAG(
    "WATCHLIST_US_MINUTE",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='*/10 9-17 * * 1-5',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='WATCHLIST_US_MINUTE',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tasks',
    data={"period": "MINUTE"},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=WATCHLIST_US_MINUTE,
)


FLUSH_HOT_WATCHLIST = DAG(
    "FLUSH_HOT_WATCHLIST",
    catchup=False,
    start_date=pendulum.datetime(2024, 7, 23, tz='Asia/Shanghai'),
    schedule_interval='0 0 18 * *',
    default_args={
        "owner": "sunfulin",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='FLUSH_HOT_WATCHLIST',
    method='PUT',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/popular/cache',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=FLUSH_HOT_WATCHLIST,
)
