import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

rock_star_accumulate_days = DAG(
    "rock_star_accumulate_days",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='rock_star_accumulate_days',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/rockerStar/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rock_star_accumulate_days,
)

rock_star_clear_accumulate_month_days = DAG(
    "rock_star_clear_accumulate_month_days",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4),
    schedule_interval='0 0 1 */1 *',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='rock_star_clear_accumulate_month_days',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/rockerStar/month/clear',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rock_star_clear_accumulate_month_days,
)

rock_star_accumulate_week_days = DAG(
    "rock_star_accumulate_week_days",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='20 9/12 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='rock_star_accumulate_week_days',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/rockerStar/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rock_star_accumulate_week_days,
)

rock_star_accumulate_week_days = DAG(
    "rock_star_accumulate_week_days",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 24, tz='Asia/Shanghai'),
    schedule_interval='20 9/12 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='rock_star_accumulate_week_days',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/rockerStar/week/days',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rock_star_accumulate_week_days,
)
