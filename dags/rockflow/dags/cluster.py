from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

from rockflow.dags.const import *

# 汇率更新
currencies_refresh = DAG(
    "currencies_refresh_by_hour",
    catchup=True,
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2022, 1, 5, 0, 0),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": timedelta(hours=1),
    }
)

SimpleHttpOperator(
    task_id='currencies_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/currencies/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=currencies_refresh,
)

# 碎股及港股更新
contracts_refresh = DAG(
    "contracts_refresh_daily",
    catchup=True,
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2022, 1, 5, 1, 0),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": timedelta(days=1),
    }
)

SimpleHttpOperator(
    task_id='contracts_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/contracts/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 3},
    dag=contracts_refresh,
)

# 实时行情聚合为1分钟
ticks = DAG(
    "ticks_by_minute",
    catchup=True,
    default_args={
        "owner": "yinxiang",
        "start_date": datetime(2022, 1, 5, 1, 0),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='ticks',
    method='PATCH',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 30},
    dag=ticks,
)
