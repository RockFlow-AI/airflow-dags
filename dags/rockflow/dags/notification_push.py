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

US_MARKET_OPEN_NOTIFICATION = DAG(
    "US_MARKET_OPEN_NOTIFICATION",
    catchup=False,
    start_date=pendulum.datetime(2022, 7, 18, tz='America/New_York'),
    schedule_interval='29 09 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='US_MARKET_OPEN_NOTIFICATION',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/tradingHours/open/notification/task?market=US',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=US_MARKET_OPEN_NOTIFICATION,
)