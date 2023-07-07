# 神策业务指标报警 - 定时任务

import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

hour_6h = "0 0,6,12,18 * * "
hour_4h = "0 7,11,15,19,23 * * "
hour_trading = "30 22,23 * * "
hour_active = "0 12,18,22 * * "

day_trading = "1-5"
day_all = "*"

setting = {
    "1": {
        "name": "sensor_alert_task_1",
        "interval": hour_6h + day_all,
        "task": "1",
    },
    "2": {
        "name": "sensor_alert_task_2",
        "interval": hour_6h + day_all,
        "task": "2",
    },
    "3": {
        "name": "sensor_alert_task_3",
        "interval": hour_6h + day_all,
        "task": "3",
    },
    "4": {
        "name": "sensor_alert_task_4",
        "interval": hour_6h + day_all,
        "task": "4",
    },
    "5": {
        "name": "sensor_alert_task_5",
        "interval": hour_6h + day_all,
        "task": "5",
    },
    "6": {
        "name": "sensor_alert_task_6",
        "interval": hour_4h + day_trading,
        "task": "6",
    },
    "7": {
        "name": "sensor_alert_task_7",
        "interval": hour_4h + day_trading,
        "task": "7",
    },
    "8_1": {
        "name": "sensor_alert_task_8_1",
        "interval": hour_trading + day_trading,
        "task": "8",
    },
    "8_2": {
        "name": "sensor_alert_task_8_2",
        "interval": "0 1 * * 2-6",
        "task": "8",
    },
    "9": {
        "name": "sensor_alert_task_9",
        "interval": hour_6h + day_all,
        "task": "9",
    },
    "10": {
        "name": "sensor_alert_task_10",
        "interval": hour_active + day_all,
        "task": "10",
    }
}

dags = []

for task, value in setting.items():
    dag = DAG(
        value['name'],
        catchup=False,
        start_date=pendulum.datetime(2023, 7, 6, tz='Asia/Shanghai'),
        schedule_interval=value['interval'],
        default_args={
            "owner": "caohaoxuan",
            "depends_on_past": False,
            "retries": 2,
            "retry_delay": timedelta(minutes=3),
        },
    )
    
    dags.append(dag)
    
    SimpleHttpOperator(
        task_id=value['name'],
        method='POST',
        http_conn_id='feishu-sensor-alert',
        endpoint='/run_task?task_id='+value["task"],
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )