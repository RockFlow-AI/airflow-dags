import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

copy_trading_copy_position = DAG(
    "copy_trading_copy_position",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='35 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_copy_position',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/init',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_copy_position,
)

copy_trading_auto_correction = DAG(
    "copy_trading_auto_correction",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='40 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction,
)