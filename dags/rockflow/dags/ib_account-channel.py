import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import timedelta

dam_account_status_query = DAG(
    "dam_account_status_query",
    catchup=False,
    start_date=pendulum.datetime(2022, 5, 18),
    schedule_interval=timedelta(minutes=30),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='dam_account_creation',
    method='POST',
    http_conn_id='flow-account-channel',
    endpoint='/inner/ib/dam/account/status',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1200},
    dag=dam_account_status_query,
)

cash_transfer_status_query_update = DAG(
    "cash_transfer_status_query_update",
    catchup=False,
    start_date=pendulum.datetime(2022, 5, 18),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='cash_transfer_status_query_update',
    method='POST',
    http_conn_id='flow-account-channel',
    endpoint='/inner/ib/dam/cash-transfer/status',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=cash_transfer_status_query_update,
)


fee_template_status_checking = DAG(
    "fee_template_status_checking",
    catchup=False,
    start_date=pendulum.datetime(2022, 6, 13),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='fee_template_status_checking',
    method='POST',
    http_conn_id='flow-account-channel',
    endpoint='/inner/ib/dam/fee-template/status/1',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=fee_template_status_checking,
)


fee_template_status_redo = DAG(
    "fee_template_status_redo",
    catchup=False,
    start_date=pendulum.datetime(2022, 6, 13),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "owner": "jingjiadong",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='fee_template_status_redo',
    method='POST',
    http_conn_id='flow-account-channel',
    endpoint='/inner/ib/dam/fee-template/status/2',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1000},
    dag=fee_template_status_redo,
)