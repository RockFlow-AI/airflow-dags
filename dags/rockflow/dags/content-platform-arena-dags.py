"""Arena Content DAGs — daily content generation

Schedule:
    arena_daily_content — 10:00 + 17:00 CST
        morning (10:00): 昨日战况回顾
        evening (17:00): 今日赛场看点

Config:
    Airflow Connection "content-platform": base URL
    Airflow Variable "CONTENT_PLATFORM_SERVICE_KEY": service API key
"""
import json

import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

_AUTH_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
}

with DAG(
    dag_id="arena_daily_content",
    catchup=False,
    start_date=pendulum.datetime(2026, 3, 24, tz="Asia/Shanghai"),
    schedule_interval="0 10,17 * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
    },
    tags=["arena", "content-platform"],
) as dag:

    SimpleHttpOperator(
        task_id="arena_daily_content",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "arena-content-{{ logical_date.strftime('%Y%m%dT%H%M') }}",
        },
        extra_options={"timeout": 300},
        data=json.dumps({
            "plan_id": "arena_daily_content",
            "input_data": {
                "arena_ids": ["r1"],
                "business_date": "{{ logical_date.in_tz('Asia/Shanghai').strftime('%Y-%m-%d') }}",
                "content_slot": "{{ 'morning' if logical_date.in_tz('Asia/Shanghai').hour < 12 else 'evening' }}",
            },
        }),
        response_check=lambda response: response.status_code in (200, 201),
        log_response=True,
    )
