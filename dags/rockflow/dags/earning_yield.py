import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 收益率排行榜_1d
earning_yield_leaderboard_update_1d = DAG(
    "earning_yield_leaderboard_update_1d",
    catchup=False,
    # utc时间1点 对应美东夏令时20点或美东冬令时21点
    start_date=pendulum.datetime(2022, 11, 4),
    schedule_interval='0 1 * * *',
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
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
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


# 获取首次入金_10min
earning_yield_first_deposit_10m = DAG(
    "earning_yield_first_deposit_10m",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_first_deposit_10m',
    method='POST',
    http_conn_id='flow-master-account',
    endpoint='/inner/masterAccounts/deposit/firstCompleted/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_first_deposit_10m,
)


# 获取所有用户首次入金_1d
earning_yield_all_first_deposit_1d = DAG(
    "earning_yield_all_first_deposit_1d",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 6 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_all_first_deposit_1d',
    method='POST',
    http_conn_id='flow-master-account',
    endpoint='/inner/masterAccounts/deposit/firstCompleted/all',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_all_first_deposit_1d,
)

# 收益率排行榜_1d
earning_yield_rate_update_1d = DAG(
    "earning_yield_rate_update_1d",
    catchup=False,
    # utc时间1点 对应美东夏令时20点或美东冬令时21点
    start_date=pendulum.datetime(2022, 11, 4),
    schedule_interval='0 1 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='earning_yield_rate_update_1d',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/leaderboards/update/rank/1d',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=earning_yield_rate_update_1d,
)

# 收益率排行榜_10min
day_earning_yield_rate_update_10m = DAG(
    "day_earning_yield_rate_update_10m",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='day_earning_yield_rate_update_10m',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/leaderboards/update/rank/10m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 200},
    dag=day_earning_yield_rate_update_10m,
)

# 获取首次入金_10min
earning_yield_first_deposit_10m = DAG(
    "earning_yield_first_deposit_10m",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_first_deposit_10m',
    method='POST',
    http_conn_id='flow-master-account',
    endpoint='/inner/masterAccounts/deposit/firstCompleted/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_first_deposit_10m,
)


# 获取所有用户首次入金_1d
earning_yield_all_first_deposit_1d = DAG(
    "earning_yield_all_first_deposit_1d",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 6 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_all_first_deposit_1d',
    method='POST',
    http_conn_id='flow-master-account',
    endpoint='/inner/masterAccounts/deposit/firstCompleted/all',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_all_first_deposit_1d,
)

# 发送交收数据补偿任务_1d
earning_yield_nlv_data_compensate_1d = DAG(
    "earning_yield_nlv_data_compensate_1d",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_nlv_data_compensate_1d',
    method='POST',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/ob/data/send/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_nlv_data_compensate_1d,
)

earning_yield_leaderboard_listing_days = DAG(
    "earning_yield_leaderboard_listing_days",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 1, tz='America/New_York'),
    schedule_interval='0 20 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='earning_yield_leaderboard_listing_days',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYields/leaderboard/listingDays',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_leaderboard_listing_days,
)
