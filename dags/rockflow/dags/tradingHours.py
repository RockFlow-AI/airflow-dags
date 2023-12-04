import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天盘前触发
trading_hours = DAG(
    "trading_hours",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 19, tz='America/New_York),
    schedule_interval='30 9 * * *',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
    }
)

SimpleHttpOperator(
    task_id='trading_hours',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/lifeCycle/sync',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=trading_hours,
)
