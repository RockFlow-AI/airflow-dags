# 运营管理机器人 - 定时任务 - 一个小时调用一次

import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

message_bot = DAG(
    "message_bot",
    catchup=False,
    start_date=pendulum.datetime(2023, 5, 30, tz='UTC'),
    schedule_interval='0 */1 * * *',
    default_args={
        "owner": "aisi",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=3),
    }
)

SimpleHttpOperator(
    task_id='message_bot',
    method='POST',
    http_conn_id='flow-admin',
    endpoint='/messages/send-expired',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=message_bot,
)

user_group_send_message_1d = DAG(
    "user_group_send_message_1d",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 1, tz='Asia/Shanghai'),
    schedule_interval='0 20 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='user_group_send_message_1d',
    method='PUT',
    http_conn_id='flow-admin',
    endpoint='/admin/inner/user/group/send/1d',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=user_group_send_message_1d,
)

user_group_send_message_1w = DAG(
    "user_group_send_message_1w",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 1, tz='Asia/Shanghai'),
    schedule_interval='0 20 * * 2',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0
    }
)


face_recognition_send_broadcast = DAG(
    "face_recognition_send_broadcast",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 14, tz='Asia/Shanghai'),
    schedule_interval='0 10 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='face_recognition_send_broadcast',
    method='PUT',
    http_conn_id='flow-admin',
    endpoint='/admin/inner/user/group/faceRecognition/send/BROADCAST',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=face_recognition_send_broadcast,
)


face_recognition_send_push = DAG(
    "face_recognition_send_push",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 14, tz='Asia/Shanghai'),
    schedule_interval='0 11 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='face_recognition_send_push',
    method='PUT',
    http_conn_id='flow-admin',
    endpoint='/admin/inner/user/group/faceRecognition/send/PUSH',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=face_recognition_send_push,
)


face_recognition_send_email = DAG(
    "face_recognition_send_email",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 14, tz='Asia/Shanghai'),
    schedule_interval='0 10 * * 5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='face_recognition_send_email',
    method='PUT',
    http_conn_id='flow-admin',
    endpoint='/admin/inner/user/group/faceRecognition/send/EMAIL',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=face_recognition_send_email,
)

user_group_send_push_broadcast = DAG(
    "user_group_send_push_broadcast",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 1, tz='Asia/Shanghai'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "momo",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='user_group_send_push_broadcast',
    method='PUT',
    http_conn_id='flow-admin',
    endpoint='/admin/inner/user/group/send/10m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=user_group_send_push_broadcast,
)