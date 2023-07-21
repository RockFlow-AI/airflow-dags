import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 12小时调用一次
clear_error_order_margin = DAG(
    "clear_error_order_margin",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 19, tz='Asia/Shanghai'),
    schedule_interval='0 */12 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='clear_error_order_margin',
    method='PUT',
    http_conn_id='flow-feed-portfolio',
    endpoint='/account/inner/clear/brokerAccount/error/orderIds',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=clear_error_order_margin,
)