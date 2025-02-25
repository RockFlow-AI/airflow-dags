import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta


option_exercise_file_simulation = DAG(
    "option_exercise_file_simulation",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 9, tz='Asia/Shanghai'),
    schedule_interval='0 10 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='option_exercise_file_simulation',
    method='PUT',
    http_conn_id='flow-file-feed-simulation',
    endpoint='/file/inner/optionExercise/task?tradeDay={date}&market=US'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 5},
    dag=option_exercise_file_simulation,
)
