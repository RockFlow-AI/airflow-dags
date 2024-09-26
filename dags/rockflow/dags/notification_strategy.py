import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


send_msg_by_config_1m = DAG(
    "send_msg_by_config_1m",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 25, tz='America/New_York'),
    schedule_interval='*/1 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='send_msg_by_config_1m',
    method='PUT',
    http_conn_id='flow-notification-strategy',
    endpoint='/notification/strategy/inner/send/config/msg',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=send_msg_by_config_1m,
)

send_telegram_msg_1m = DAG(
    "send_telegram_msg_1m",
    catchup=False,
    start_date=pendulum.datetime(2024, 8, 25, tz='America/New_York'),
    schedule_interval='*/1 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='send_telegram_msg_1m',
    method='PUT',
    http_conn_id='flow-notification-strategy',
    endpoint='/notification/telegram/inner/send/scheduled/messages',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=send_telegram_msg_1m,
)