import json
import time
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

task_time = int(round(time.time() * 1000))

interest_path = '/ledger/inner/interest/task?time={task_time}'.format(task_time=task_time)

ledger_interest_daily_calculator = DAG(
    "ledger_interest_daily_calculator",
    catchup=False,
    start_date=datetime(2022, 10, 19, 0, 0),
    schedule_interval='0 8 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='ledger_interest_daily_calculator',
    method='POST',
    http_conn_id='flow-ledger',
    endpoint=interest_path,
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ledger_interest_daily_calculator,
)
