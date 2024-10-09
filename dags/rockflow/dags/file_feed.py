import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta


option_exercise_file = DAG(
    "option_exercise_file",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 9, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='option_exercise_file',
    method='PUT',
    http_conn_id='flow-file-feed',
    endpoint='/file/inner/optionExercise/task?tradeDay={date}&market=US'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 5},
    dag=option_exercise_file,
)
