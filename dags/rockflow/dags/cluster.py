import time
import pendulum
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
    schedule_interval='*/1 * * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

ticks_on_time = SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?time={{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ticks,
)

ticks_delay_1m = SimpleHttpOperator(
    task_id='ticks_delay_1m',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?time={{ (macros.datetime.fromisoformat(ts) - macros.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S") }}',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ticks,
)

ticks_on_time.pre_execute = lambda **x: time.sleep(10)
ticks_on_time.post_execute = lambda **x: time.sleep(10)

ticks_on_time >> ticks_delay_1m


# 1分钟行情聚合为5分钟
ticks_5m = DAG(
    "ticks_by_5_minutes",
    catchup=False,
    start_date=datetime(2022, 2, 23, 0, 0),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='ticks_5m',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks?timeframe=5m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=ticks_5m,
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

# 实时行情收盘后OHLC数据落库
daily_last_tick_hk = DAG(
    "daily_last_tick_hk",
    catchup=False,
    start_date=pendulum.datetime(2022, 2, 28, tz='Asia/Hong_Kong'),
    schedule_interval='30 16 * * 1-5',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

ticks_on_time = SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/ticks/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_tick_hk,
)

daily_last_tick_us = DAG(
    "daily_last_tick_us",
    catchup=False,
    start_date=pendulum.datetime(2022, 2, 28, tz='America/New_York'),
    schedule_interval='30 20 * * 1-5',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)
ticks_on_time = SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/ticks/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_tick_us,
)

# 三方前一天美股tick数据dev
DEV_daily_previous_tick_us = DAG(
    "DEV_daily_previous_tick_us",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 8, tz='America/New_York'),
    schedule_interval='45 22 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DEV_daily_previous_tick_us',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/inner/kline/ld?step=2&timeframe=1d&adjustment=forward&markets=US',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 100},
    dag=DEV_daily_previous_tick_us,
)


# 三方前一天美股tick数据prod
daily_previous_tick_us = DAG(
    "daily_previous_tick_us",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 8, tz='America/New_York'),
    schedule_interval='30 22 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='daily_previous_tick_us',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/inner/kline/ld?step=2&timeframe=1d&adjustment=forward&markets=US',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 100},
    dag=daily_previous_tick_us,
)


# 三方前一天港股tick数据dev
DEV_daily_previous_tick_hk = DAG(
    "DEV_daily_previous_tick_hk",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 8, tz='Asia/Hong_Kong'),
    schedule_interval='30 20 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DEV_daily_previous_tick_hk',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/inner/kline/ld?step=2&timeframe=1d&adjustment=forward&markets=HK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 100},
    dag=DEV_daily_previous_tick_hk,
)


# 三方前一天港股tick数据dev
daily_previous_tick_hk = DAG(
    "daily_previous_tick_hk",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 8, tz='Asia/Hong_Kong'),
    schedule_interval='15 20 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='daily_previous_tick_hk',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/inner/kline/ld?step=2&timeframe=1d&adjustment=forward&markets=HK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 100},
    dag=daily_previous_tick_hk,
)

# 日结单
daily_statement = DAG(
    "daily_statement",
    catchup=False,
    start_date=datetime(2022, 3, 10, 0, 0),
    schedule_interval=timedelta(hours=1),
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 3,
    }
)

SimpleHttpOperator(
    task_id='daily_statement',
    method='GET',
    http_conn_id='flow-statement-service',
    endpoint='/inner/statements/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=daily_statement,
)