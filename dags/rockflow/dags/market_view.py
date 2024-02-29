import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


DELAY_MARKET_TASK = DAG(
    "DELAY_MARKET_TASK",
    catchup=False,
    start_date=pendulum.datetime(2024, 2, 29, tz='Asia/Shanghai'),
    schedule_interval='30 18 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='DELAY_MARKET_TASK',
    method='POST',
    http_conn_id='flow-master-account',
    endpoint='/inner/masterAccounts/delayMarket/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=DELAY_MARKET_TASK,
)