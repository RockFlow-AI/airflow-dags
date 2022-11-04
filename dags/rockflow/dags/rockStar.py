
import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator


rock_star_accumulate_days = DAG(
    "rock_star_accumulate_days",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='35 20 * * 1-5',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='RockStarAccumulateDays',
    method='GET',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/rockerStar/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag="rock_star_accumulate_days",
)



