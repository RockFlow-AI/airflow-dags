from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from rockflow.common.hkex import HKEX
from rockflow.common.utils import default_proxy

args = {
    'owner': 'daijunkai',
}

dag = DAG(
    dag_id='update_tickers',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)


def get_all_nasdaq():
    proxy = default_proxy()
    print(f"call Nasdaq, proxy: {proxy}")
    # Nasdaq(proxy=proxy)._get()
    HKEX(proxy=proxy)._get()


run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=get_all_nasdaq,
    dag=dag
)
