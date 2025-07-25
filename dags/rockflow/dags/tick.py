import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 实时行情聚合为1分钟
ticks = DAG(
    "ticks_by_minute",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval='*/1 * * * *',
    default_args={
        "owner": "jingjiadong",
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
    endpoint='/ticker/inner/ticks?time={{ (macros.datetime.fromisoformat(ts) - macros.timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S") }}',
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

# 5分钟行情聚合为10分钟
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
    start_date=pendulum.datetime(2022, 5, 21, tz='America/New_York'),
    schedule_interval='0 12 * * 6',
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

# 实时行情收盘后OHLC数据落库 -- ticker-service

# 实时行情收盘后OHLC数据落库 -- aggregation
daily_last_tick_hk_aggregation = DAG(
    "daily_last_tick_hk_aggregation",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 2, tz='Asia/Hong_Kong'),
    schedule_interval='30 16 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/ticks/latest/LAST_TICK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_tick_hk_aggregation,
)

daily_last_tick_us_aggregation = DAG(
    "daily_last_tick_us_aggregation",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 2, tz='America/New_York'),
    schedule_interval='30 20 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/ticks/latest/LAST_TICK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_tick_us_aggregation,
)

daily_last_tick_us_option_aggregation = DAG(
    "daily_last_tick_us_option_aggregation",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 2, tz='America/New_York'),
    schedule_interval='30 20 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/OSUS/ticks/latest/LAST_TICK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_tick_us_option_aggregation,
)

# 最后一分钟补点
daily_last_minute_hk_aggregation = DAG(
    "daily_last_minute_hk_aggregation",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 2, tz='Asia/Hong_Kong'),
    schedule_interval='1 16 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/ticks/latest/LAST_MINUTE',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_minute_hk_aggregation,
)

daily_last_minute_us_aggregation = DAG(
    "daily_last_minute_us_aggregation",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 2, tz='America/New_York'),
    schedule_interval='1 16 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/ticks/latest/LAST_MINUTE',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_last_minute_us_aggregation,
)

# 美股期权分片缓存过期清理
option_osus_sharding_symbols_housekeeping = DAG(
    "option_osus_sharding_symbols_housekeeping",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 5, tz='America/New_York'),
    schedule_interval='0 1 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='DELETE',
    http_conn_id='flow-mr-aggregation',
    endpoint='/aggregation/inner/housekeeping/expiration/OSUS/symbols',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_osus_sharding_symbols_housekeeping,
)

money_box_tick_update = DAG(
    "money_box_tick_update",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 25, tz='Asia/Shanghai'),
    schedule_interval='0 9 * * *',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='money-box',
    method='PUT',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/money-box/tick/MB0001%7CRF',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=money_box_tick_update,
)


# EOD数据修正 - yahoo爬取 HK
daily_last_tick_hk_yahoo = DAG(
    "daily_last_tick_hk_yahoo",
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 14, tz='Asia/Hong_Kong'),
    schedule_interval='45 16 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/ticks/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1200},
    dag=daily_last_tick_hk_yahoo,
)

# EOD数据修正 - yahoo爬取 US
daily_last_tick_us_yahoo = DAG(
    "daily_last_tick_us_yahoo",
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 14, tz='America/New_York'),
    schedule_interval='50 16,17 * * 1-5',
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/ticks/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1200},
    dag=daily_last_tick_us_yahoo,
)

scrape_money_box_price = DAG(
    "scrape_money_box_price",
    catchup=False,
    start_date=pendulum.datetime(2023, 10, 10, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='scrape_price',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/daily-ticks/auto/insert/dailyTick/MB0001%7CRF',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=scrape_money_box_price,
)

daily_corporate_actions = DAG(
    "daily_corporate_actions",
    catchup=False,
    start_date=pendulum.datetime(2023, 10, 27, tz='America/New_York'),
    schedule_interval='0 2 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
    }
)

corporate_actions_today = SimpleHttpOperator(
    task_id='daily_corporate_actions_today',
    method='GET',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/corporateActions',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_corporate_actions,
)

corporate_actions_future = SimpleHttpOperator(
    task_id='daily_corporate_actions_future',
    method='GET',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/corporateActions?daysInFuture=10',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_corporate_actions,
)

corporate_actions_today >> corporate_actions_future

update_better_buys_year_yield = DAG(
    "update_better_buys_year_yield",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 4, tz='America/New_York'),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='update_better_buys_year_yield',
    method='PUT',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/options/update/better/yield',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=update_better_buys_year_yield,
)


check_holiday_then_notice = DAG(
    "check_holiday_then_notice",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 9, tz='Asia/Shanghai'),
    schedule_interval='30 9 * * *',
    default_args={
        "owner": "mao",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='check_holiday_then_notice',
    method='GET',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/holidays',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=check_holiday_then_notice,
)