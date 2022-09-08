import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


TAG_WATCHLIST_US_DAY = DAG(
    "TAG_WATCHLIST_US_DAY",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='0 0 * * 2-6',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='TAG_WATCHLIST_US_DAY',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tag-watchlist/tasks?period=DAY',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=TAG_WATCHLIST_US_DAY,
)

TAG_WATCHLIST_US_MINUTE = DAG(
    "TAG_WATCHLIST_US_MINUTE",
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
    task_id='TAG_WATCHLIST_US_MINUTE',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tag-watchlist/tasks?period=MINUTE',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=TAG_WATCHLIST_US_MINUTE,
)

