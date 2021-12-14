from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

from rockflow.dags.const import *

cluster_timing_dag = DAG(
    "cluster_timing",
    default_args={
        "owner": "yinxiang",
        # "start_date": datetime(2021, 12, 8),
        "start_date": datetime.now(),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        # "schedule_interval": "@once",  # for debug
        "schedule_interval": "@once",  # for debug
    }
)

SimpleHttpOperator(
    task_id='currencies_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/currencies/refresh',
    dag=cluster_timing_dag,
)
