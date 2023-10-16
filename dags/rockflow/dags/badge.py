import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

BADGE_US_MINUTE = DAG(
    "BADGE_US_MINUTE",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='BADGE_US_MINUTE',
    method='PUT',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/badge/own/task',
    data={"period": "MINUTE"},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=BADGE_US_MINUTE,
)

