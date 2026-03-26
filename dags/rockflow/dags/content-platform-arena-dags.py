"""Arena Content DAGs — daily content generation

Schedule:
    arena_morning_content — 10:15 CST: 昨日战况回顾
    arena_evening_content — 18:00 CST: 今日赛场看点

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

_DEFAULT_ARGS = {
    "owner": "tanqiwen",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=3),
}


def _arena_task(*, content_slot: str) -> SimpleHttpOperator:
    return SimpleHttpOperator(
        task_id="run",
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
                "content_slot": content_slot,
            },
        }),
        response_check=lambda response: (
            response.status_code in (200, 201)
            and response.json().get("status") != "FAILED"
        ),
        log_response=True,
    )


with DAG(
    dag_id="arena_morning_content",
    catchup=False,
    start_date=pendulum.datetime(2026, 3, 24, tz="Asia/Shanghai"),
    schedule_interval="15 10 * * *",
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["arena", "content-platform"],
):
    _arena_task(content_slot="morning")

with DAG(
    dag_id="arena_evening_content",
    catchup=False,
    start_date=pendulum.datetime(2026, 3, 24, tz="Asia/Shanghai"),
    schedule_interval="0 18 * * *",
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["arena", "content-platform"],
):
    _arena_task(content_slot="evening")
