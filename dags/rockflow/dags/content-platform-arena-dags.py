"""Arena Unified DAG — single pipeline for battle report + chat digest.

Schedule: 10:00 / 15:00 / 18:00 CST

One pipeline, one LLM call, one coherent story.
The battle report's community_discussion previews what the chat digest expands on.

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

with DAG(
    dag_id="arena_content",
    catchup=False,
    start_date=pendulum.datetime(2026, 3, 24, tz="Asia/Shanghai"),
    schedule_interval="0 10,15,18 * * *",
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["arena", "content-platform"],
):
    unified_run = SimpleHttpOperator(
        task_id="unified_run",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "arena-unified-{{ logical_date.strftime('%Y%m%dT%H%M') }}",
        },
        extra_options={"timeout": 600},
        data=json.dumps({
            "plan_id": "arena_unified",
            "input_data": {
                "arena_ids": ["r2"],
                "business_date": "{{ logical_date.in_tz('Asia/Shanghai').strftime('%Y-%m-%d') }}",
                "content_slot": "{{ logical_date.in_tz('Asia/Shanghai').strftime('%H') }}",
            },
        }),
        response_check=lambda response: (
            response.status_code in (200, 201)
            and response.json().get("status") != "FAILED"
        ),
        log_response=True,
    )
