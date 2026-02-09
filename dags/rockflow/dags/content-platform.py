"""Content Platform DAGs — thin scheduling declarations

All business logic lives in the backend (content-platform API).
DAGs only declare schedules and HTTP calls.

News:
  news_analyze_minute  — every 2 min, fetch + analyze + translate
  news_digest_12h      — every 12h, generate daily digest

Stock:
  stock_daily          — 06:00 CST, metrics + SEO for top 1000 tickers
  stock_weekly         — Sunday 04:00 CST, analysis
  stock_monthly        — 1st 03:00 CST, identity
  stock_quarterly      — Jan/Apr/Jul/Oct 1st 02:00 CST, financials

Config:
  Airflow Connection "content-platform": base URL
  Airflow Variable "CONTENT_PLATFORM_SERVICE_KEY": service API key
"""
import json

import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

_AUTH_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
}

_STOCK_DEFAULT_ARGS = {
    "owner": "tanqiwen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}


# ===========================================================================
# News: news_analyze_minute (every 2 min)
# ===========================================================================

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
        endpoint="/runs",
        headers={
            **_AUTH_HEADERS,
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


# ===========================================================================
# News: news_digest_12h (07:00 + 19:00 CST)
# ===========================================================================

with DAG(
    dag_id="news_digest_12h",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 7,19 * * *",
    max_active_runs=1,
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
        endpoint="/runs",
        headers={
            **_AUTH_HEADERS,
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


# ===========================================================================
# Stock: stock_daily (06:00 CST)
# ===========================================================================

with DAG(
    dag_id="stock_daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 6 * * *",
    max_active_runs=1,
    default_args=_STOCK_DEFAULT_ARGS,
    tags=["stock", "content-platform", "daily"],
) as dag_daily:

    fetch_tickers = SimpleHttpOperator(
        task_id="fetch_tickers",
        method="GET",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/top-tickers",
        request_params={"limit": "1000"},
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.status_code == 200,
        response_filter=lambda r: json.dumps(r.json()["tickers"]),
        log_response=True,
    )

    index_metrics = SimpleHttpOperator(
        task_id="index_metrics",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/runs",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_daily-stock_index_metrics-SPY-{{ ds }}",
        },
        data=json.dumps({"plan_id": "stock_index_metrics", "input_data": {"ticker": "SPY"}}),
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        log_response=True,
    )

    submit_metrics = SimpleHttpOperator(
        task_id="submit_metrics",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_daily-stock_metrics-{{ ds }}",
        },
        data='{"plan_id": "stock_metrics", "tickers": {{ task_instance.xcom_pull(task_ids="fetch_tickers") }}}',
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_metrics = HttpSensor(
        task_id="poll_metrics",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_metrics') }}",
            "max_fail_pct": "5",
        },
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=120 * 60,
        mode="reschedule",
    )

    submit_seo = SimpleHttpOperator(
        task_id="submit_seo",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_daily-stock_seo-{{ ds }}",
        },
        data='{"plan_id": "stock_seo", "tickers": {{ task_instance.xcom_pull(task_ids="fetch_tickers") }}}',
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_seo = HttpSensor(
        task_id="poll_seo",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_seo') }}",
            "max_fail_pct": "5",
        },
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    )

    [fetch_tickers, index_metrics] >> submit_metrics >> poll_metrics >> submit_seo >> poll_seo


# ===========================================================================
# Stock: stock_weekly (Sunday 04:00 CST)
# ===========================================================================

with DAG(
    dag_id="stock_weekly",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 4 * * 0",
    max_active_runs=1,
    default_args=_STOCK_DEFAULT_ARGS,
    tags=["stock", "content-platform", "weekly"],
) as dag_weekly:

    submit_analysis = SimpleHttpOperator(
        task_id="submit_analysis",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_weekly-stock_analysis-{{ ds }}",
        },
        data=json.dumps({"plan_id": "stock_analysis", "ticker_limit": 1000}),
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_analysis = HttpSensor(
        task_id="poll_analysis",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_analysis') }}",
            "max_fail_pct": "15",
        },
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=360 * 60,
        mode="reschedule",
    )

    submit_analysis >> poll_analysis


# ===========================================================================
# Stock: stock_monthly (1st of month 03:00 CST)
# ===========================================================================

with DAG(
    dag_id="stock_monthly",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 3 1 * *",
    max_active_runs=1,
    default_args=_STOCK_DEFAULT_ARGS,
    tags=["stock", "content-platform", "monthly"],
) as dag_monthly:

    submit_identity = SimpleHttpOperator(
        task_id="submit_identity",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_monthly-stock_identity-{{ ds }}",
        },
        data=json.dumps({"plan_id": "stock_identity", "ticker_limit": 1000}),
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_identity = HttpSensor(
        task_id="poll_identity",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_identity') }}",
            "max_fail_pct": "5",
        },
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=180 * 60,
        mode="reschedule",
    )

    submit_identity >> poll_identity


# ===========================================================================
# Stock: stock_quarterly (Jan/Apr/Jul/Oct 1st 02:00 CST)
# ===========================================================================

with DAG(
    dag_id="stock_quarterly",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 6, tz="Asia/Shanghai"),
    schedule_interval="0 2 1 1,4,7,10 *",
    max_active_runs=1,
    default_args=_STOCK_DEFAULT_ARGS,
    tags=["stock", "content-platform", "quarterly"],
) as dag_quarterly:

    submit_financials = SimpleHttpOperator(
        task_id="submit_financials",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": "stock_quarterly-stock_financials-{{ ds }}",
        },
        data=json.dumps({"plan_id": "stock_financials", "ticker_limit": 1000}),
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_financials = HttpSensor(
        task_id="poll_financials",
        http_conn_id="content-platform",
        endpoint="/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_financials') }}",
            "max_fail_pct": "5",
        },
        headers={"Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}"},
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=120 * 60,
        mode="reschedule",
    )

    submit_financials >> poll_financials
