import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


fetch_kline_hk_daily = DAG(
    "fetch_kline_hk_daily",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='Asia/Shanghai'),
    schedule_interval='10 16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_hk_daily',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/candles/DAILY/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_hk_daily,
)

fetch_kline_us_daily = DAG(
    "fetch_kline_us_daily",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='05 16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_us_daily',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/candles/DAILY/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_us_daily,
)

fetch_kline_osus_daily = DAG(
    "fetch_kline_osus_daily",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='05 16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_osus_daily',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/OSUS/candles/DAILY/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_osus_daily,
)

fetch_kline_hk_week = DAG(
    "fetch_kline_hk_week",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='Asia/Shanghai'),
    schedule_interval='0 9 * * 1',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_hk_week',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/candles/WEEK/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_hk_week,
)

fetch_kline_us_week = DAG(
    "fetch_kline_us_week",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='0 9 * * 1',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_us_week',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/candles/WEEK/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_us_week,
)

fetch_kline_osus_week = DAG(
    "fetch_kline_osus_week",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='0 9 * * 1',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_osus_week',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/OSUS/candles/WEEK/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_osus_week,
)

#  港股 9点半～16点 START
fetch_kline_hk_1m_9_30_9_59 = DAG(
    "fetch_kline_hk_1m_9_30_9_59",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='Asia/Shanghai'),
    schedule_interval='30-59 9 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_hk_1m_9_30_9_59',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_hk_1m_9_30_9_59,
)
# --
fetch_kline_hk_1m_10_00_16_00 = DAG(
    "fetch_kline_hk_1m_10_00_16_00",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='Asia/Shanghai'),
    schedule_interval='* 10-16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_hk_1m_10_00_16_00',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/HK/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_hk_1m_10_00_16_00,
)
#  港股 9点半～16点 END

#  美股 9点半～16点 START
fetch_kline_us_1m_9_30_9_59 = DAG(
    "fetch_kline_us_1m_9_30_9_59",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='30-59 9 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_us_1m_9_30_9_59',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_us_1m_9_30_9_59,
)

fetch_kline_us_1m_10_00_16_00 = DAG(
    "fetch_kline_us_1m_10_00_16_00",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='* 10-16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_us_1m_10_00_16_00',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/US/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_us_1m_10_00_16_00,
)
#  美股 9点半～16点 END

#  美股期权 9点半～16点 START
fetch_kline_osus_1m_9_30_9_59 = DAG(
    "fetch_kline_osus_1m_9_30_9_59",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='30-59 9 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_osus_1m_9_30_9_59',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/OSUS/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_osus_1m_9_30_9_59,
)


fetch_kline_osus_1m_10_00_16_00 = DAG(
    "fetch_kline_osus_1m_10_00_16_00",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 7, tz='America/New_York'),
    schedule_interval='* 10-16 * * 1-5',
    default_args={
        "owner": "maoshanghui",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='fetch_kline_osus_1m_10_00_16_00',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/markets/OSUS/candles/ONE_MINUTE/latest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=fetch_kline_osus_1m_10_00_16_00,
)
# 美股期权 9点半～16点 END