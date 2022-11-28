from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

portfolio_route_transaction_alert = DAG(
    "portfolio_route_transaction_alert",
    catchup=False,
    start_date=datetime(2022, 10, 13, 0, 0),
    schedule_interval='30 8 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='portfolio_route_transaction_alert',
    method='PUT',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/transaction/alert',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=portfolio_route_transaction_alert,
)