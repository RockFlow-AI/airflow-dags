import time
import pendulum
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 三方支付卡验证弹框&资产待办引导
order_card_popUp = DAG(
    "order_card_popUp",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 10, tz='Asia/Hong_Kong'),
    schedule_interval='*/30 * * * * ?',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='order_card_popUp',
    method='POST',
    http_conn_id='flow-order',
    endpoint='/order/inner/order/cards/popUp',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=order_card_popUp,
)