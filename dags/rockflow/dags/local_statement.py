import pendulum as pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator


us_ledger_local_statement = DAG(
    "us_ledger_local_statement",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 27, tz='America/New_York'),
    schedule_interval='0 0 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 16,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='us_ledger_local_statement',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/statement/markets/US/{date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=us_ledger_local_statement,
)


hk_ledger_local_statement = DAG(
    "hk_ledger_local_statement",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 27, tz='Asia/Shanghai'),
    schedule_interval='0 0 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 16,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='hk_ledger_local_statement',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/statement/markets/US/{date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=hk_ledger_local_statement,
)