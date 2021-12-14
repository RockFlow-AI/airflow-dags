from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

from rockflow.dags.const import *

currencies_refresh = DAG(
    "currencies_refresh",
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2021, 12, 15),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": "@hourly",
    }
)

SimpleHttpOperator(
    task_id='currencies_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/currencies/refresh',
    dag=currencies_refresh,
)

contracts_refresh = DAG(
    "contracts_refresh",
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2021, 12, 15),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": "@hourly",
    }
)

SimpleHttpOperator(
    task_id='contracts_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/contracts/refresh',
    dag=contracts_refresh,
)

ticks = DAG(
    "ticks",
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2021, 12, 15),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": "* * * * *",
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks',
    dag=ticks,
)
