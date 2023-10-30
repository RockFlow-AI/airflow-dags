import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 理财产品
place_orders = DAG(
    "place_orders_fund",
    catchup=False,
    start_date=pendulum.datetime(2023, 10, 30, tz='Asia/Shanghai'),
    schedule_interval='0 12 * * 0',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
    }
)

SimpleHttpOperator(
    task_id='place_orders',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/products/orders',
    headers={'appId': '1'},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=place_orders,
)
