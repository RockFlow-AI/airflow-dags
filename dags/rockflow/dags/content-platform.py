"""Content Platform DAGs — thin scheduling declarations

All business logic lives in the backend (content-platform API).
DAGs only declare schedules and HTTP calls.

News:
  news_analyze_minute  — every 2 min, fetch + analyze + translate
  news_digest_12h      — every 12h, generate daily digest

Stock:
  stock_daily                — 06:00 CST, metrics (all) + SEO (progressive rollout)
  stock_weekly               — Sunday 04:00 CST, analysis
  stock_monthly              — 1st 03:00 CST, identity
  stock_quarterly            — Jan/Apr/Jul/Oct 1st 02:00 CST, financials
Config:
  Airflow Connection "content-platform": base URL
  Airflow Variable "CONTENT_PLATFORM_SERVICE_KEY": service API key
  Airflow Variable "SEO_WAVE_SIZE": tickers added per week (default 100)
  Airflow Variable "SEO_LAUNCH_DATE": rollout start date (ISO format)
"""
import json

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor


# DAG 文件单独部署到 airflow,不带 sibling module — helpers 必须 inline。
_AUTH_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
}

_READ_AUTH_HEADERS = {
    "Authorization": "Bearer {{ var.value.CONTENT_PLATFORM_SERVICE_KEY }}",
}

_IDEM_KEY = "{{ dag.dag_id }}-{{ task.task_id }}-{{ run_id }}"


def _check_run_terminal(response):
    """sensor response_check.

    DONE → True (sensor 通过)
    FAILED / CANCELED → raise AirflowException (立刻 fail, 不等 timeout)
    其他 (PENDING / RUNNING / WAITING) → False (继续 poll)
    """
    status = response.json()["status"]
    if status == "DONE":
        return True
    if status in ("FAILED", "CANCELED"):
        raise AirflowException(f"run terminated with status={status}")
    return False


def submit_and_wait(
    name: str,
    plan_id: str,
    input_data: dict,
    timeout: int = 1800,
    poke_interval: int = 30,
):
    """Submit content-platform run + sensor wait — returns ``(submit, poll)``.

    POST ``/api/runs`` 立即返回 PENDING run_id; HttpSensor poll
    ``/api/runs/{run_id}`` 直到 DONE / FAILED / CANCELED.
    下游必须 ref ``poll`` (sensor terminal) 而非 ``submit``, 否则跳过等待。
    """
    submit = SimpleHttpOperator(
        task_id=f"submit_{name}",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={**_AUTH_HEADERS, "Idempotency-Key": _IDEM_KEY},
        data=json.dumps({"plan_id": plan_id, "input_data": input_data}),
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: r.json()["run_id"],
        log_response=True,
    )
    poll = HttpSensor(
        task_id=f"poll_{name}",
        http_conn_id="content-platform",
        endpoint=(
            "/api/runs/{{ task_instance.xcom_pull(task_ids='submit_"
            + name
            + "') }}"
        ),
        headers=_AUTH_HEADERS,
        response_check=_check_run_terminal,
        poke_interval=poke_interval,
        timeout=timeout,
        mode="reschedule",
    )
    submit >> poll
    return submit, poll

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

    # timeout/poke 基于 prod 实测:full-pipeline p50≈160s / p95≈470s;1200s 覆盖
    # backend watchdog 一次 self-heal (15min stuck threshold + retry),false-positive≈0,
    # 真 hang 时 alarm 比默认 30min 快 10min。15s poke 把 DONE 检测延迟从 15s 降到 7.5s。
    submit_and_wait(
        "news_analyze", "news_analyze",
        {"limit": 50, "target_locales": ["zh_hans", "zh_hant"]},
        timeout=1200,
        poke_interval=15,
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

    submit_and_wait(
        "news_digest", "news_digest",
        {"limit": 100, "target_locales": ["zh_hans", "zh_hant"]},
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
        endpoint="/api/internal/stocks/top-tickers?limit=1000",
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.status_code == 200,
        response_filter=lambda r: json.dumps(r.json()["tickers"]),
        log_response=True,
    )

    fetch_seo_tickers = SimpleHttpOperator(
        task_id="fetch_seo_tickers",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/seo-tickers",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data=(
            '{"tickers": {{ task_instance.xcom_pull(task_ids="fetch_tickers") }},'
            ' "effective_date": "{{ ds }}",'
            ' "wave_size": {{ var.value.SEO_WAVE_SIZE }},'
            ' "launch_date": "{{ var.value.SEO_LAUNCH_DATE }}"}'
        ),
        extra_options={"timeout": 60},
        response_check=lambda r: r.status_code == 200,
        response_filter=lambda r: json.dumps(r.json()["tickers"]),
        log_response=True,
    )

    seo_gate = ShortCircuitOperator(
        task_id="seo_gate",
        python_callable=lambda **ctx: bool(
            json.loads(ctx["task_instance"].xcom_pull(task_ids="fetch_seo_tickers") or "[]")
        ),
    )

    # async via submit_and_wait: returns (submit_op, poll_op);
    # 下游必须 ref poll 而非 submit, 否则 stock_metrics 在 index_metrics 真正完成前就启动
    _, poll_index_metrics = submit_and_wait(
        "index_metrics", "stock_index_metrics", {"ticker": "SPY"},
    )

    submit_metrics = SimpleHttpOperator(
        task_id="submit_metrics",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
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
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_metrics') }}",
            "max_fail_pct": "5",
        },
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=120 * 60,
        mode="reschedule",
    )

    submit_seo = SimpleHttpOperator(
        task_id="submit_seo",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data='{"plan_id": "stock_seo", "tickers": {{ task_instance.xcom_pull(task_ids="fetch_seo_tickers") }}}',
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )

    poll_seo = HttpSensor(
        task_id="poll_seo",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_seo') }}",
            "max_fail_pct": "5",
        },
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    )

    # Metrics branch: all tickers
    # ref poll_index_metrics (sensor terminal) — 不是 submit, 否则 submit_metrics
    # 在 stock_index_metrics pipeline 真正完成前就启动, 读到旧 stock.index_metrics
    [fetch_tickers, poll_index_metrics] >> submit_metrics >> poll_metrics
    # SEO branch: rollout-gated tickers
    fetch_tickers >> fetch_seo_tickers >> seo_gate
    [poll_metrics, seo_gate] >> submit_seo >> poll_seo


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
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
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
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_analysis') }}",
            "max_fail_pct": "15",
        },
        headers=_READ_AUTH_HEADERS,
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
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
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
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_identity') }}",
            "max_fail_pct": "5",
        },
        headers=_READ_AUTH_HEADERS,
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
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
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
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_financials') }}",
            "max_fail_pct": "5",
        },
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=30,
        timeout=120 * 60,
        mode="reschedule",
    )

    submit_financials >> poll_financials


# ===========================================================================
# News: news_submit_google_sitemap (every 2 hours)
# ===========================================================================

with DAG(
    dag_id="news_submit_google_sitemap",
    catchup=False,
    start_date=pendulum.datetime(2026, 2, 28, tz="Asia/Shanghai"),
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["news", "content-platform", "seo"],
) as dag_google:

    SimpleHttpOperator(
        task_id="submit_google_sitemap",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/news/submit-google-sitemap",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        extra_options={"timeout": 60},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )


# ===========================================================================
# Dashboard: bobby_daily_user_activity daily backfill (06:30 CST)
#
# Fact table powering the v4.1 dashboard — retention / feedback /
# attribution penetration all derive from it. Prior to this DAG the
# table was maintained by hand-run backfill scripts, so data routinely
# fell a few days behind and dashboard metrics underreported.
#
# Window is [ds-2, ds+2) ≈ 4 local days centred on the scheduled run,
# wide enough to absorb Mongo ETL lag from rockbot and to heal partial
# prior runs (upsert is idempotent). The admin endpoint caps window
# at 14 days, so a wider catch-up needs the CLI script.
# ===========================================================================

with DAG(
    dag_id="dashboard_dua_daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 24, tz="Asia/Shanghai"),
    schedule_interval="30 6 * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["dashboard", "content-platform", "daily"],
) as dag_dua:

    SimpleHttpOperator(
        task_id="backfill_dua",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/admin/dashboard/backfill-dua",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data=json.dumps({
            "from_date": "{{ macros.ds_add(ds, -2) }}",
            "to_date": "{{ macros.ds_add(ds, 2) }}",
        }),
        extra_options={"timeout": 180},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )


# ===========================================================================
# Dashboard: SA attribution app-DAU daily top-up (06:35 CST)
#
# Keeps ``sa_daily_app_metrics`` current so the attribution dashboard's
# denominator doesn't go stale — engaged_penetration = engaged_dau /
# app_dau, and app_dau comes from this table. ``user_deposit_profile``
# stays on a separate weekly cadence because a full refresh is ≈4 min
# (9 weekly Mongo scans) — too heavy for daily.
#
# Runs 5 minutes after dashboard_dua_daily so if both fail the same
# day we can tell which side actually broke from the Airflow timeline.
# ===========================================================================

with DAG(
    dag_id="dashboard_attribution_daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 24, tz="Asia/Shanghai"),
    schedule_interval="35 6 * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["dashboard", "content-platform", "daily"],
) as dag_attribution:

    SimpleHttpOperator(
        task_id="backfill_sa_daily",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/admin/dashboard/backfill-sa-daily",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data=json.dumps({"days": 3}),
        extra_options={"timeout": 60},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )


# ===========================================================================
# Dashboard: SA user_deposit_profile weekly refresh (Sunday 07:00 CST)
#
# Separate from dashboard_attribution_daily because a full profile
# refresh scans SA events week by week for 60 days, ≈4 minutes. A
# user's deposit bucket is a slow-moving attribute (people don't jump
# between "undeposit" and ">5k" daily), so weekly cadence keeps the
# segment overlays accurate without spending 4 minutes per day on
# nearly identical work.
#
# Runs Sunday at 07:00 CST — after the daily DAGs finish, before the
# weekly dashboard review window typically starts.
# ===========================================================================

with DAG(
    dag_id="dashboard_attribution_profile_weekly",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 26, tz="Asia/Shanghai"),
    schedule_interval="0 7 * * 0",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=15),
    },
    tags=["dashboard", "content-platform", "weekly"],
) as dag_attribution_profile:

    SimpleHttpOperator(
        task_id="backfill_sa_profile",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/admin/dashboard/backfill-sa-profile",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data=json.dumps({"lookback_days": 60}),
        # Endpoint itself runs ≈4min; give HTTP ample margin so a slow
        # SA query doesn't timeout the whole task on an otherwise
        # healthy run.
        extra_options={"timeout": 600},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )


# ===========================================================================
# Dashboard: bobby_filled_orders daily backfill (06:40 CST)
#
# Powers the Bobby 成交金额 (Bottom card) and OrdersDistribution
# dashboards. portfolio-service is the source of truth; this DAG keeps
# ``bobby_filled_orders`` current. Prior to this DAG the table was
# populated only by hand-run script — it stalled for 7 days
# (2026-04-21 → 2026-04-28) before anyone noticed the dashboard hadn't
# moved.
#
# Window is "last 3 Asia/Shanghai days" (the script's default overlap),
# wide enough that a row whose ``updateTime`` shifts mid-run on
# portfolio-service's offset-paginated API gets re-pulled in subsequent
# runs. Upsert is stale-safe (newer ``update_time`` wins) so consecutive
# runs only converge.
#
# Scheduled 5 minutes after dashboard_attribution_daily so the daily
# backfill chain (DUA → SA → orders) lines up on the timeline — easier
# triage when one fails.
# ===========================================================================

with DAG(
    dag_id="dashboard_filled_orders_daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 28, tz="Asia/Shanghai"),
    schedule_interval="40 6 * * *",
    max_active_runs=1,
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["dashboard", "content-platform", "daily"],
) as dag_filled_orders:

    SimpleHttpOperator(
        task_id="backfill_filled_orders",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/admin/dashboard/backfill-filled-orders",
        headers={
            **_AUTH_HEADERS,
            "Idempotency-Key": _IDEM_KEY,
        },
        data=json.dumps({"days": 3}),
        # 3-day window = ~15 portfolio-service pages × ~3s each + upsert
        # batches; settles in well under 2 minutes. 180s timeout absorbs
        # the slow-SA-day case without masking a genuinely stuck run.
        extra_options={"timeout": 180},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )
