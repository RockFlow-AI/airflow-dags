import pendulum as pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator


MAINLAND_SUPPLEMENTARY_INFO_REMINDER_TASK = DAG(
    "MAINLAND_SUPPLEMENTARY_INFO_REMINDER_TASK",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 17, tz='Asia/Shanghai'),
    schedule_interval='0 21 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 16,
        "retry_delay": timedelta(minutes=10)
    }
)

SimpleHttpOperator(
    task_id='MAINLAND_SUPPLEMENTARY_INFO_REMINDER_TASK',
    method='PUT',
    http_conn_id='flow-ledger',
    endpoint='masterAccount/inner/supplementaryInfo/mainland/reminder/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 36000},
    dag=MAINLAND_SUPPLEMENTARY_INFO_REMINDER_TASK,
)