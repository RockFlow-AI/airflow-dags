import time
import pendulum
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 行情卡续订
quote_card_renew = DAG(
    "quote_card_renew_month",
    catchup=False,
    start_date=pendulum.datetime(2022, 6, 1, tz='America/New_York'),
    schedule_interval='0 0 */2 1,2 * *',
    default_args={
        "owner": "hujing",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='quote_card_renew',
    method='POST',
    http_conn_id='flow-shop',
    endpoint='/shop/inner/quoteCard/renew',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=quote_card_renew,
)