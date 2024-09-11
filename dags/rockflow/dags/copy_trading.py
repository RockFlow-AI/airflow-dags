import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 盘前5分钟跟单任务扫描
copy_trading_init_position = DAG(
    "copy_trading_init_position",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='35 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='copy_trading_init_position',
    method='GET',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/init',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=copy_trading_init_position,
)

copy_trading_correct_position = DAG(
    "copy_trading_correct_position",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='40 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='copy_trading_correct_position',
    method='GET',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=copy_trading_correct_position,
)