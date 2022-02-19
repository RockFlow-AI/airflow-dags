from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 汇率更新
currencies_refresh = DAG(
    "currencies_refresh_by_hour",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval=timedelta(hours=1),
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='currencies_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/currencies/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=currencies_refresh,
)

# 碎股及港股更新
contracts_refresh = DAG(
    "contracts_refresh_daily",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval=timedelta(days=1),
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='contracts_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/contracts/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 3},
    dag=contracts_refresh,
)

# 实时行情聚合为1分钟
ticks = DAG(
    "ticks_by_minute",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ticks,
)

# 1分钟行情聚合为10分钟
ticks_10m = DAG(
    "ticks_by_10_minutes",
    catchup=False,
    start_date=datetime(2022, 2, 10, 0, 0),
    schedule_interval=timedelta(minutes=10),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_10m',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=10m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 500},
    dag=ticks_10m,
)


# 1分钟行情聚合为15分钟
ticks_15m = DAG(
    "ticks_by_15_minutes",
    catchup=False,
    start_date=datetime(2022, 2, 10, 0, 0),
    schedule_interval=timedelta(minutes=15),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_15m',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=15m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=ticks_15m,
)

# 15分钟行情聚合为30钟
ticks_30m = DAG(
    "ticks_by_30_minutes",
    catchup=False,
    start_date=datetime(2022, 2, 10, 0, 0),
    schedule_interval=timedelta(minutes=30),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_30m',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=30m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=ticks_30m,
)

# 30分钟行情聚合为1小时
ticks_1h = DAG(
    "ticks_by_1_hours",
    catchup=False,
    start_date=datetime(2022, 2, 10, 0, 0),
    schedule_interval=timedelta(minutes=60),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_1h',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=1h',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=ticks_1h,
)


# 1小时行情聚合为4小时
ticks_4h = DAG(
    "ticks_by_4_hours",
    catchup=False,
    start_date=datetime(2022, 2, 10, 0, 0),
    schedule_interval=timedelta(hours=4),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_4h',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=4h',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=ticks_4h,
)

# 天级行情聚合为周级行情
ticks_1w = DAG(
    "ticks_by_1_week",
    catchup=False,
    start_date=datetime(2022, 2, 12, 12, 0),
    schedule_interval=timedelta(hours=168),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_1w',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=1w',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=ticks_1w,
)

# 解析ice数据
tick_ice_1d = DAG(
    "ticks_resolve_ice_1d",
    catchup=False,
    start_date=datetime(2022, 2, 19, 20, 0),
    schedule_interval=timedelta(hours=24),
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='tick_ice_1d',
    method='GET',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ice',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60000},
    dag=tick_ice_1d,
)