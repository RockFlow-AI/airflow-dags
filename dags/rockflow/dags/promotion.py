import time
from datetime import datetime, timedelta

import pendulum
from dateutil.relativedelta import relativedelta
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

check_iqiyi_award_status = DAG(
    "check_iqiyi_award_status",
    catchup=False,
    start_date=pendulum.datetime(2023, 4, 20, tz='UTC'),
    schedule_interval='0 10 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='check_iqiyi_award_status',
    method='POST',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/task/activity/iqiyi/issueStatus',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=check_iqiyi_award_status,
)

# 定时灌水，六小时一次
auto_add_landing_page_count = DAG(
    "auto_add_landing_page_count",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 20, tz='Asia/Shanghai'),
    schedule_interval='0 */6 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='auto_add_landing_page_count',
    method='PUT',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/fund/qualification/auto/add',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=auto_add_landing_page_count,
)

promotion_reward_claim = DAG(
    "promotion_reward_claim",
    catchup=False,
    start_date=pendulum.datetime(2024, 2, 4, tz='Asia/Shanghai'),
    schedule_interval='0 * * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='promotion_reward_claim',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/rewards',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=promotion_reward_claim,
)

coupon_expires = DAG(
    "coupon_expires",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 20, tz='America/New_York'),
    schedule_interval='0 9 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='coupon_expires',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/coupon/expired',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=coupon_expires,
)

coupon_expiring_soon_push = DAG(
    "coupon_expiring_soon_push",
    catchup=False,
    start_date=pendulum.datetime(2025, 2, 20, tz='America/New_York'),
    schedule_interval='0 8 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='coupon_expiring_soon_push',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/coupon/expiring/soon/push',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=coupon_expiring_soon_push,
)

update_coupon_pending_status = DAG(
    "update_coupon_pending_status",
    catchup=False,
    start_date=pendulum.datetime(2025, 4, 1, tz='America/New_York'),
    schedule_interval='0 0 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='update_coupon_pending_status',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/coupon/pending/status',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=update_coupon_pending_status,
)

add_leaderboard_fake_data = DAG(
    "add_leaderboard_fake_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 4, 4, tz='Asia/Shanghai'),
    schedule_interval='0 9,15 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='add_leaderboard_fake_data',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/leaderboard/fake/50',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=add_leaderboard_fake_data,
)

distribute_QUIZ_coupon_reward = DAG(
    "distribute_QUIZ_coupon_reward",
    catchup=False,
    start_date=pendulum.datetime(2024, 4, 4, tz='Asia/Shanghai'),
    schedule_interval='0 0 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='distribute_QUIZ_coupon_reward',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/api/reward/QUIZ_REWARD/QUEST_USER_QUIZ',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=distribute_QUIZ_coupon_reward,
)


DISTRIBUTE_BOBBY_ORDER_COMMISSION_REDUCTION_REWARD = DAG(
    "DISTRIBUTE_BOBBY_ORDER_COMMISSION_REDUCTION_REWARD",
    catchup=False,
    start_date=pendulum.datetime(2025, 8, 7, tz='Asia/Shanghai'),
    schedule_interval='0 14 1 1,4,7,10 *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='DISTRIBUTE_BOBBY_ORDER_COMMISSION_REDUCTION_REWARD',
    method='PATCH',
    http_conn_id='flow-promotion',
    endpoint='/promotion/api/reward/BOBBY_ORDER_COMMISSION_REDUCTION/QUEST_BOBBY_ORDER?startDay={startDay}&endDay={endDay}'
    .format(startDay=(datetime.now() - relativedelta(months=3)).strftime("%Y-%m-%d"), endDay=(datetime.now()).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=DISTRIBUTE_BOBBY_ORDER_COMMISSION_REDUCTION_REWARD,
)


BOBBY_ORDER_COMMISSION_REDUCTION_LEADERBOARD = DAG(
    "BOBBY_ORDER_COMMISSION_REDUCTION_LEADERBOARD",
    catchup=False,
    start_date=pendulum.datetime(2025, 8, 7, tz='Asia/Shanghai'),
    schedule_interval='0 0 1 * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='BOBBY_ORDER_COMMISSION_REDUCTION_LEADERBOARD',
    method='PUT',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/promotions/BOBBY_ORDER_COMMISSION_REDUCTION/participation/leaderboard?month={month}'
    .format(month=(datetime.now()).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=BOBBY_ORDER_COMMISSION_REDUCTION_LEADERBOARD,
)