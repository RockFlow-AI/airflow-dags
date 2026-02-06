"""News DAGs

news_analyze_minute: Every 2 minutes, fetch + analyze + translate latest news.
news_digest_12h: Every 12 hours, generate daily news digest.

Auth: Service API Key from Airflow Variable "CONTENT_PLATFORM_SERVICE_KEY"
      Injected via Jinja template at task execution time (not DAG parse time).
"""
import json
import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# --- news_analyze: every 2 minutes ---

with DAG(
    dag_id="news_analyze_minute",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="*/2 * * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=30),
    },
    tags=["news", "content-platform"],
) as dag_analyze:

    SimpleHttpOperator(
        task_id="news_analyze",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
            "Idempotency-Key": "news-analyze-{{ logical_date.strftime('%Y%m%dT%H%M') }}",
        },
        extra_options={"timeout": 300},
        data=json.dumps({
            "plan_id": "news_analyze",
            "input_data": {
                "limit": 50,
                "target_locales": ["zh_hans", "zh_hant"],
            },
        }),
        response_check=lambda response: response.status_code in (200, 201),
        log_response=True,
    )

# --- news_digest: every 12 hours ---

with DAG(
    dag_id="news_digest_12h",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 7,19 * * *",  # 07:00 and 19:00 CST
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
    },
    tags=["news", "content-platform"],
) as dag_digest:

    SimpleHttpOperator(
        task_id="news_digest",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
            "Idempotency-Key": "news-digest-{{ logical_date.strftime('%Y%m%dT%H%M') }}",
        },
        extra_options={"timeout": 120},
        data=json.dumps({
            "plan_id": "news_digest",
            "input_data": {
                "limit": 100,
                "target_locales": ["zh_hans", "zh_hant"],
            },
        }),
        response_check=lambda response: response.status_code in (200, 201),
        log_response=True,
    )
