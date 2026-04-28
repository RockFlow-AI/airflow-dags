import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

rfs_statement = DAG(
    "rfs_statement",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 10, tz='Asia/Shanghai'),
    schedule_interval='00 21 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='rfs_statement',
    method='POST',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/rfs/export?statementDay={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rfs_statement,
)

import_statement = DAG(
    "import_statement",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 10, tz='Asia/Shanghai'),
    schedule_interval='00 20 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='import_statement',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/import?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)