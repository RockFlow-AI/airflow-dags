import time
from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 理财产品
GENERATE_PRODUCT_TASK = DAG(
    "GENERATE_PRODUCT_TASK",
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 14, tz='Asia/Shanghai'),
    schedule_interval='30 11 * * 7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

create_product = SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/products/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=GENERATE_PRODUCT_TASK,
)

place_order = SimpleHttpOperator(
    task_id='place_orders',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/products/orders',
    headers={'appId': '1'},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=GENERATE_PRODUCT_TASK,
)

create_product.post_execute = lambda **x: time.sleep(30)
create_product >> place_order

VIRTUAL_ORDER_TASK = DAG(
    "VIRTUAL_ORDER_TASK",
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 14, tz='Asia/Shanghai'),
    schedule_interval='30 */2 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/products/virtualOrder',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=VIRTUAL_ORDER_TASK,
)

PATROL_ORDER_TASK = DAG(
    "PATROL_ORDER_TASK",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 28, tz='America/New_York'),
    schedule_interval='0 10 * * 5',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/products/patrol',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=PATROL_ORDER_TASK,
)
