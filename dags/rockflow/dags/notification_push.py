import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


DAILY_HALA_ALL_SENDING = DAG(
    "DAILY_HALA_ALL_SENDING",
    catchup=False,
    start_date=pendulum.datetime(2022, 7, 14, tz='Asia/Shanghai'),
    schedule_interval='0 22 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DAILY_HALA_ALL_SENDING',
    method='POST',
    http_conn_id='flow-notification',
    endpoint='/push/inner/task/all/days/HALA',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=DAILY_HALA_ALL_SENDING,
)