import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 收益率排行榜_1d
earning_yield_leaderboard_update_1d = DAG(
    "earning_yield_leaderboard_update_1d",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='32 20 * * 1-5',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='earning_yield_leaderboard_update_1d',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/leaderboards/update/1d',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_leaderboard_update_1d,
)

# 收益率排行榜_10min
earning_yield_leaderboard_update_10m = DAG(
    "earning_yield_leaderboard_update_10m",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='32/10 20 * * 1-5',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='earning_yield_leaderboard_update_10m',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/leaderboards/update/10m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_leaderboard_update_10m,
)
