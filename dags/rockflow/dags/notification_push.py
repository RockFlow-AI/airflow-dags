import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import timedelta


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

DAILY_HALA_2_ALL_SENDING = DAG(
    "DAILY_HALA_2_ALL_SENDING",
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 14, tz='Asia/Shanghai'),
    schedule_interval='0 15 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DAILY_HALA_2_ALL_SENDING',
    method='POST',
    http_conn_id='flow-notification',
    endpoint='/push/inner/task/all/days/HALA_2_',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=DAILY_HALA_2_ALL_SENDING,
)


DAILY_DUSK_ALL_SENDING = DAG(
    "DAILY_DUSK_ALL_SENDING",
    catchup=False,
    start_date=pendulum.datetime(2022, 7, 29, tz='Asia/Shanghai'),
    schedule_interval='30 18 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DAILY_DUSK_ALL_SENDING',
    method='POST',
    http_conn_id='flow-notification',
    endpoint='/push/inner/task/all/days/DUSK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=DAILY_DUSK_ALL_SENDING,
)

DAILY_NOON_ALL_SENDING = DAG(
    "DAILY_NOON_ALL_SENDING",
    catchup=False,
    start_date=pendulum.datetime(2022, 7, 30, tz='Asia/Shanghai'),
    schedule_interval='30 12 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DAILY_NOON_ALL_SENDING',
    method='POST',
    http_conn_id='flow-notification',
    endpoint='/push/inner/task/all/days/NOON',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=DAILY_NOON_ALL_SENDING,
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

# 定时任务 - 每十分钟对 24 小时内未注册用户发送 push
push_to_unregister = DAG(
    "push_to_unregister",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 30, tz='Asia/Shanghai'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='push_to_unregister',
    method='POST',
    http_conn_id='flow-notification',
    endpoint='/notification/inner/task/push/unregister',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=push_to_unregister,
)


# 定时任务 每分钟检测
PUSH_TO_LIMIT_ORDER_UNSETTLED = DAG(
    "push_to_limit_order_unsettled",
    catchup=False,
    start_date=pendulum.datetime(2024, 8, 28, tz='Asia/Shanghai'),
    schedule_interval='* * * * *',
    default_args={
        "owner": "sunfulin",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='push_to_limit_order_unsettled',
    method='PATCH',
    http_conn_id='flow-notification',
    endpoint='/portfolio/inner/pendingOrders?interval=15&session=0&source=HK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=push_to_unregister,
)
#
# SimpleHttpOperator(
#     task_id='push_to_limit_order_unsettled',
#     method='PATCH',
#     http_conn_id='flow-notification',
#     endpoint='/portfolio/inner/pendingOrders?interval=60&session=0&source=HK',
#     response_check=lambda response: response.json()['code'] == 200,
#     extra_options={"timeout": 200},
#     dag=push_to_unregister,
# )
#
# SimpleHttpOperator(
#     task_id='push_to_limit_order_unsettled',
#     method='PATCH',
#     http_conn_id='flow-notification',
#     endpoint='/portfolio/inner/pendingOrders?interval=15&session=0&source=US',
#     response_check=lambda response: response.json()['code'] == 200,
#     extra_options={"timeout": 200},
#     dag=push_to_unregister,
# )
#
# SimpleHttpOperator(
#     task_id='push_to_limit_order_unsettled',
#     method='PATCH',
#     http_conn_id='flow-notification',
#     endpoint='/portfolio/inner/pendingOrders?interval=15&session=1&source=US',
#     response_check=lambda response: response.json()['code'] == 200,
#     extra_options={"timeout": 200},
#     dag=push_to_unregister,
# )
#
# SimpleHttpOperator(
#     task_id='push_to_limit_order_unsettled',
#     method='PATCH',
#     http_conn_id='flow-notification',
#     endpoint='/portfolio/inner/pendingOrders?interval=60&session=0&source=US',
#     response_check=lambda response: response.json()['code'] == 200,
#     extra_options={"timeout": 200},
#     dag=push_to_unregister,
# )
#
# SimpleHttpOperator(
#     task_id='push_to_limit_order_unsettled',
#     method='PATCH',
#     http_conn_id='flow-notification',
#     endpoint='/portfolio/inner/pendingOrders?interval=60&session=1&source=US',
#     response_check=lambda response: response.json()['code'] == 200,
#     extra_options={"timeout": 200},
#     dag=push_to_unregister,
# )


