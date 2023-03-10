from airflow.models import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

option_exercise_alert = DAG(
    "option_exercise_alert",
    catchup=False,
    start_date=datetime(2022, 12, 5, 0, 0),
    schedule_interval='45 12 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='option_exercise_alert',
    method='PUT',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/optionExercise/alert',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_exercise_alert,
)