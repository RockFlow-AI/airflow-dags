from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "arena_bobby_analyse",
    catchup=False,
    start_date=pendulum.datetime(2025, 11, 8),
    schedule_interval="* * * * *",
    default_args={
        "owner": "xuwenzhe",
        "depends_on_past": False,
        "retries": 0,
    },
) as arena_bobby_analyse:
    arena_participants_analyse_task = SimpleHttpOperator(
        task_id="arena_participants_analyse_process",
        method="GET",
        http_conn_id="tradebot",
        endpoint="/bot/api/arena/participants_analyse/process",
        response_check=lambda response: response.json().get("code") == 200,
    )
