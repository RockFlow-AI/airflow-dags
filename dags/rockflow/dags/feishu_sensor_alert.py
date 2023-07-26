# TradeGPT自动化airflow配置文件

import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

tradeGPT_run_initial = DAG(
    "tradeGPT_run_initial",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="0 10 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_run_initial',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/initial",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_run_initial,
)

tradeGPT_run_second = DAG(
    "tradeGPT_run_second",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="20 18 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_run_second',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/second",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_run_second,
)

tradeGPT_send_notification = DAG(
    "tradeGPT_send_notification",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="0 16,17,18 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_send_notification',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/send_notification",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_send_notification,
)

tradeGPT_option_update_1 = DAG(
    "tradeGPT_option_update_1",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="2-56/10 21 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_option_update_1',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/option_update/run_option_update",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_option_update_1,
)

tradeGPT_option_update_2 = DAG(
    "tradeGPT_option_update_2",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="2-56/30 22-23 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_option_update_2',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/option_update/run_option_update",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_option_update_2,
)

tradeGPT_option_update_3 = DAG(
    "tradeGPT_option_update_3",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 17, tz='Asia/Shanghai'),
    schedule_interval="2-56/30 0-4 * * 1-5",
    default_args={
        "owner": "huangdexi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)

SimpleHttpOperator(
    task_id='tradeGPT_option_update_3',
    method='GET',
    http_conn_id='tradegpt',
    endpoint="/option_update/run_option_update",
    response_check=lambda response: response.json()['code'] == 200,
    dag=tradeGPT_option_update_3,
)