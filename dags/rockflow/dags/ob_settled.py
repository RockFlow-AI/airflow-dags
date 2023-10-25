import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta


ob_settled_stat = DAG(
    "ob_settled_stat",
    catchup=False,
    start_date=pendulum.datetime(2023, 4, 20, tz='UTC'),
    schedule_interval='30 8 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='ob_settled_stat',
    method='POST',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/ob/stat/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 10},
    dag=ob_settled_stat,
)


auto_trade_match = DAG(
    "auto_trade_match",
    catchup=False,
    start_date=pendulum.datetime(2023, 4, 20, tz='UTC'),
    schedule_interval='30 10 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='auto_trade_match',
    method='POST',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/tradeMatch/auto',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=auto_trade_match,
)