import pendulum
from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

leaderboard_baseData_alarm = DAG(
    "leaderboard_baseData_alarm",
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 9, tz='Asia/Shanghai'),
    schedule_interval='10,20 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='leaderboard_baseData_alarm',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/leaderboard/update/alarm/baseData',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=leaderboard_baseData_alarm,
)

leaderboard_daily_cutoff_alarm = DAG(
    "leaderboard_daily_cutoff_alarm",
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 9, tz='America/New_York'),
    schedule_interval='10,20 4 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='leaderboard_daily_cutoff_alarm',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/leaderboard/update/alarm/leaderboard',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=leaderboard_daily_cutoff_alarm,
)

earningYield_leaderboard_alert = DAG(
    "earningYield_leaderboard_alert",
    catchup=False,
    start_date=pendulum.datetime(2025, 9, 4, tz='America/New_York'),
    schedule_interval='30 4,19,20 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='earningYield_leaderboard_alert',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYields/alert/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earningYield_leaderboard_alert,
)

earningYield_realtime_task = DAG(
    "earningYield_realtime_task",
    catchup=False,
    start_date=pendulum.datetime(2025, 12, 30, tz='America/New_York'),
    schedule_interval='58 19 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='earningYield_realtime_task',
    method='POST',
    http_conn_id='flow-feed-portfolio',
    endpoint='portfolio/inner/earningYield/realtime/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earningYield_realtime_task,
)

earningYield_leaderboard_statistics = DAG(
    "earningYield_leaderboard_statistics",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='earningYield_leaderboard_statistics',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/leaderboards/rank/statistics',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earningYield_leaderboard_statistics,
)