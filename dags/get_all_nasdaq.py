from __future__ import print_function

import os
import sys
import time
from builtins import range
from datetime import datetime, timedelta
from pprint import pprint

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from nasdaq import Nasdaq

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

seven_days_ago = datetime.combine(
    datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'daijunkai',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='get_all_nasdaq', default_args=args,
    schedule_interval=None)


def get_all_nasdaq(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)
    Nasdaq()._get()


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)

# Generate 10 sleeping tasks, sleeping from 0 to 9 seconds respectively
for i in range(2):
    task = PythonOperator(
        task_id='sleep_for_' + str(i),
        python_callable=get_all_nasdaq,
        op_kwargs={'random_base': float(i) / 10},
        dag=dag)

    task.set_upstream(run_this)
