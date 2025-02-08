import pendulum
from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

account_asset_sync_us = DAG(
    "account_asset_sync_us",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 4, tz='America/New_York'),
    schedule_interval='0 0 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='account_asset_sync_us',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/accounts/sync/accountAssets/activeUsers?startTime={date}'.format(date=(datetime.now() + timedelta(days=-5)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=account_asset_sync_us,
)

account_asset_sync_hk = DAG(
    "account_asset_sync_hk",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 4, tz='Asia/Shanghai'),
    schedule_interval='0 0 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='account_asset_sync_hk',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/accounts/sync/accountAssets/activeUsers?startTime={date}'.format(date=(datetime.now() + timedelta(days=-10)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=account_asset_sync_hk,
)
