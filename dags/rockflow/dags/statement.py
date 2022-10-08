from airflow.models import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator


statements_daily = DAG(
    "ledger_statement_by_daily",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval='0 0 8 * * *',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0,
    }
)

statements_on_time = SimpleHttpOperator(
    task_id='statements_daily',
    method='PATCH',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/statement/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=statements_daily,
)