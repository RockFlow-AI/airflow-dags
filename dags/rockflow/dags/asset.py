import pendulum
from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天北京时间六点调用一次
asset = DAG(
    "asset",
    catchup=False,
    start_date=pendulum.datetime(2023, 6, 29, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='asset',
    method='POST',
    http_conn_id='flow-feed-portfolio',
    endpoint='/account/inner/calculate/asset',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=asset,
)

# 刷新存钱罐资产
refresh_money_box = DAG(
    "refresh_money_box",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 12, tz='Asia/Shanghai'),
    schedule_interval='0 */6 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='refresh_money_box',
    method='POST',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/money-box/refreshMoneyBox?date={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=refresh_money_box,
)


asset_stat = DAG(
    "asset_stat",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 12, tz='America/New_York'),
    schedule_interval='30 20 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='asset_stat',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='ledger/inner/accountAsset/stat/markets/US/{date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=asset_stat,
)


yield_stat = DAG(
    "yield_stat",
    catchup=False,
    start_date=pendulum.datetime(2025, 4, 12, tz='Asia/Shanghai'),
    schedule_interval='0 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='yield_stat',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='ledger/inner/accountAsset/stat/earningYield/markets/US/{date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=yield_stat,
)