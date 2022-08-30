from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from rockflow.dags.const import *
from rockflow.operators.symbol import *

position_estimate_overwrite = DAG(
    "position_estimate_overwrite",
    catchup=False,
    start_date=pendulum.datetime(2022, 8, 19, tz='America/New_York'),
    schedule_interval='10 20 * * 0-5',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

position_estimate_overwrite_task = SimpleHttpOperator(
    task_id='position_estimate_overwrite',
    method='PATCH',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/positions',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=position_estimate_overwrite,
)
