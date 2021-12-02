import os
import sys
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from nasdaq import Nasdaq

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

args = {
    'owner': 'daijunkai',
}

dag = DAG(
    dag_id='get_all_nasdaq',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)


def get_all_nasdaq():
    print("call Nasdaq")
    Nasdaq()._get()


run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=get_all_nasdaq,
    dag=dag)
