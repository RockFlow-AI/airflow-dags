import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


# 定时任务 - 每三小时调用一次拉取富途财报数据
load_futu_financial_report = DAG(
    "load_futu_financial_report",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='0 */3 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='load_futu_financial_report',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/loadFUTUSymbolReports',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=load_futu_financial_report,
)

# 定时任务 - 每天加载未发布的数据到 redis
load_financial_report_to_redis = DAG(
    "load_financial_report_to_redis",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='0 10 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='load_financial_report_to_redis',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/refreshSymbols',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=load_financial_report_to_redis,
)

# 定时任务 - 每天拉取 fmp 数据
load_fmp_financial_report_data = DAG(
    "load_fmp_financial_report_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='*/1 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='load_fmp_financial_report_data',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/refreshRevenueReport',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=load_fmp_financial_report_data,
)

# 定时任务 - 每十分钟拉取活跃股票 fmp 数据
load_active_symbol_fmp_data = DAG(
    "load_active_symbol_fmp_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='load_active_symbol_fmp_data',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/refreshPublishReport',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=load_active_symbol_fmp_data,
)

# 定时任务 - 每小时更新财报看涨看跌比例
refresh_active_symbol_proportion = DAG(
    "refresh_active_symbol_proportion",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='0 */1 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='refresh_active_symbol_proportion',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/refreshSymbolProportion',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=refresh_active_symbol_proportion,
)

# 定时任务 - 每三小时调用一次拉取富途财报详情页数据
load_futu_financial_report_page_info = DAG(
    "load_futu_financial_report_page_info",
    catchup=False,
    start_date=pendulum.datetime(2024, 2, 24, tz='Asia/Shanghai'),
    schedule_interval='0 */3 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='load_futu_financial_report_page_info',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/loadFUTUReportInfos',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=load_futu_financial_report_page_info,
)

# 定时任务 - 每天 23:00 刷新 yahoo 未平仓量
reload_yahoo_openInterest = DAG(
    "reload_yahoo_openInterest",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 19, tz='Asia/Shanghai'),
    schedule_interval='0 23 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)

SimpleHttpOperator(
    task_id='reload_yahoo_openInterest',
    method='PUT',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/financial/report/inner/openInterest',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 360000},
    dag=reload_yahoo_openInterest,
)