import pytz
from airflow.models import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

corporate_action_lark = DAG(
    "corporate_action_lark",
    catchup=False,
    start_date=pendulum.datetime(2026, 1, 22, tz='America/New_York'),
    schedule_interval='0,30 3 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='corporate_action_lark',
    method='PATCH',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/corporateActions/route/lark',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=corporate_action_lark,
)