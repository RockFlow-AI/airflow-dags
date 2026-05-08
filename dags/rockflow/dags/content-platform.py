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

Tags:
  ``envelope`` — feeds CP /api/internal/records/{kind} consumed directly
  by AT (agent-toolkit) retrievers. Distinct from stock_daily (which
  feeds Bobby /api/public/stocks/{ticker} snapshot path).

Backfill historical dates (NOT via DAG):
  Daily DAGs only run for "today" (catchup=False). To populate
  envelope tables for a past as_of_date (e.g. dogfood / disaster
  recovery), call ``POST /api/internal/stocks/submit-pipeline`` directly
  with ``{"plan_id": "<plan>", "tickers": [...], "as_of_date":
  "YYYY-MM-DD"}``. Returns batch_ids; poll
  ``/api/internal/stocks/batch-status``. Single-shot plans (master) use
  ``POST /api/runs`` with ``{"plan_id": "<plan>", "input_data":
  {"as_of_date": "..."}}``.

HK plans first-deploy prerequisite:
  ``stock_hk_news`` / ``stock_hk_financials`` / ``stock_hk_price_history``
  / ``stock_hk_southbound`` resolve InnerCode internally via
  ``stock.hk_master_ticker`` records — fail-fast if master snapshot is
  not yet published. Each per-ticker HK DAG opens with an HttpSensor
  hitting ``/api/internal/stocks/hk-coverage``: 200 → proceed; 404 →
  sensor times out (5min) and DAG fails with a clear "trigger
  stock_hk_master first" signal. After hk_master runs once, sensor
  passes immediately on every subsequent daily run.

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


# ===========================================================================
# M1 CN_A (A 股) DAGs
# ===========================================================================
#
# 6 DAGs aligned with M1 v2.7 proposal:
#   stock_cn_a_master            monthly Sun 02:00 (single-run via /api/runs)
#   stock_cn_a_concept           weekly Sun 03:00 (per-ticker fan-out)
#   stock_cn_a_capital_flow      daily 22:00 (per-ticker fan-out + summary)
#   stock_cn_a_northbound        daily 18:30 (per-ticker fan-out + summary)
#   stock_cn_a_earnings_preview  daily 19:00 (per-ticker fan-out)
#   stock_cn_a_aggregate         daily 22:30 (per-ticker fan-out, sensor-gated
#                                             on capital_flow + northbound summaries)
#
# All schedules in Asia/Shanghai (jydb is Asia/Shanghai). Per-ticker plans
# use submit-pipeline with cn_a master snapshot ticker source (no tickers
# list passed). Master plan single-run via /api/runs because input shape
# is {"as_of_date": ds} not {"ticker": t}.

_VENDOR_PLAN_DEFAULT_ARGS = {
    "owner": "tanqiwen",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}


def _wait_hk_master_published():
    """Sensor — gate HK per-ticker fan-out on hk master snapshot existence.

    Hits ``/api/internal/stocks/hk-coverage``:
      200 → master snapshot published, proceed immediately
      404 → not yet published, sensor 5min timeout then DAG fail-fast

    First-deploy guardrail: ``stock_hk_news`` / ``stock_hk_financials`` /
    ``stock_hk_southbound`` / ``stock_hk_price_history`` fetch ops resolve
    jydb InnerCode via ``stock.hk_master_ticker`` records. Master miss →
    100 % ERROR_FATAL across the batch (silent ETL gap). Sensor turns
    silent batch-wide failure into explicit "trigger stock_hk_master DAG
    first" log signal.

    Once ``stock_hk_master`` has been triggered once, hk-coverage stays
    200 forever (singleton record), so this sensor is no-cost on steady
    state — pass on first poke.
    """
    return HttpSensor(
        task_id="wait_hk_master_published",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/hk-coverage",
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.status_code == 200,
        poke_interval=60,
        timeout=5 * 60,
        mode="reschedule",
    )


def _submit_pipeline_fanout(
    name: str,
    plan_id: str,
    *,
    ticker_limit: int = 6000,
    timeout: int = 60 * 60,
):
    """Submit fan-out plan (no tickers list — endpoint pulls from master /
    FMP US-equity screener depending on plan).

    Returns (submit, poll). Sensor terminates when batch reaches terminal
    state. max_fail_pct=10 absorbs sub-record EMPTY (eg ticker not in HSGT
    universe for northbound) which the plan routes through save_op for R5
    audit and counts as success.

    Args:
        ticker_limit: Cap on tickers fanned out per run. Default 6000 covers
            the cn_a universe (jydb ~5500 active) and HK universe (~2500).
            US-only plans (e.g. stock_us_financials, W6 slice 2) override
            to 1000 to align with stock_daily / SEO universe cap.
    """
    submit = SimpleHttpOperator(
        task_id=f"submit_{name}",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/submit-pipeline",
        headers={**_AUTH_HEADERS, "Idempotency-Key": _IDEM_KEY},
        data=json.dumps({
            "plan_id": plan_id,
            "ticker_limit": ticker_limit,
            "as_of_date": "{{ ds }}",     # explicit Asia/Shanghai trading date
            "ds": "{{ ds }}",
            "dag_id": "{{ dag.dag_id }}",
        }),
        extra_options={"timeout": 300},
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: ",".join(r.json()["batch_ids"]),
        log_response=True,
    )
    poll = HttpSensor(
        task_id=f"poll_{name}",
        http_conn_id="content-platform",
        endpoint="/api/internal/stocks/batch-status",
        request_params={
            "batch_ids": "{{ task_instance.xcom_pull(task_ids='submit_"
                         + name + "') }}",
            "max_fail_pct": "10",
        },
        headers=_READ_AUTH_HEADERS,
        response_check=lambda r: r.json().get("terminal", False),
        poke_interval=60,
        timeout=timeout,
        mode="reschedule",
    )
    submit >> poll
    return submit, poll


# ─── stock_cn_a_master (monthly Sun 02:00) ─────────────────────────────

with DAG(
    dag_id="stock_cn_a_master",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    # 1st Sunday of each month at 02:00 — schedule_interval cron handles
    # day-of-week and day-of-month conjunction
    schedule_interval="0 2 1-7 * 0",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "monthly"],
) as dag_cn_a_master:
    submit_master = SimpleHttpOperator(
        task_id="submit_master",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={**_AUTH_HEADERS, "Idempotency-Key": _IDEM_KEY},
        data=json.dumps({
            "plan_id": "stock_cn_a_master",
            "input_data": {"as_of_date": "{{ ds }}"},
        }),
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: r.json()["run_id"],
        log_response=True,
    )
    poll_master = HttpSensor(
        task_id="poll_master",
        http_conn_id="content-platform",
        endpoint=(
            "/api/runs/{{ task_instance.xcom_pull(task_ids='submit_master') }}"
        ),
        headers=_AUTH_HEADERS,
        response_check=_check_run_terminal,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    submit_master >> poll_master


# ─── stock_cn_a_concept (weekly Sun 03:00) ─────────────────────────────

with DAG(
    dag_id="stock_cn_a_concept",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    schedule_interval="0 3 * * 0",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "weekly"],
) as dag_cn_a_concept:
    _submit_pipeline_fanout("concept", "stock_cn_a_concept", timeout=2 * 60 * 60)


# ─── stock_cn_a_capital_flow (daily 22:00) ─────────────────────────────

with DAG(
    dag_id="stock_cn_a_capital_flow",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    schedule_interval="0 22 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily"],
) as dag_cn_a_capital_flow:
    _submit_pipeline_fanout(
        "capital_flow", "stock_cn_a_capital_flow", timeout=60 * 60,
    )


# ─── stock_cn_a_northbound (daily 18:30) ───────────────────────────────

with DAG(
    dag_id="stock_cn_a_northbound",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    schedule_interval="30 18 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily"],
) as dag_cn_a_northbound:
    _submit_pipeline_fanout(
        "northbound", "stock_cn_a_northbound", timeout=60 * 60,
    )


# ─── stock_cn_a_earnings_preview (daily 19:00) ─────────────────────────

with DAG(
    dag_id="stock_cn_a_earnings_preview",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    schedule_interval="0 19 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily"],
) as dag_cn_a_earnings_preview:
    _submit_pipeline_fanout(
        "earnings_preview", "stock_cn_a_earnings_preview", timeout=60 * 60,
    )


# ─── stock_cn_a_aggregate (daily 22:30, gated on upstream summaries) ───
#
# Sensor-based dependency on capital_flow + northbound terminal sensor tasks.
# Schedule offset (22:30) gives capital_flow (22:00) ~30min budget; northbound
# (18:30) typically completes well before. Aggregate inside the plan still
# runs against the latest sub-record per block, so sensor failure (eg
# upstream timed out) shouldn't block today's aggregate — the plan EMPTY
# routes terminal without write, ops can investigate.

from airflow.sensors.external_task import ExternalTaskSensor  # noqa: E402

with DAG(
    dag_id="stock_cn_a_aggregate",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 6, tz="Asia/Shanghai"),
    schedule_interval="30 22 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily"],
) as dag_cn_a_aggregate:
    # Wait for upstream poll sensors (terminal task in capital_flow + northbound
    # DAGs). All 3 DAGs share daily schedule so ExternalTaskSensor's default
    # logical_date matching works (same ds).
    wait_capital_flow = ExternalTaskSensor(
        task_id="wait_capital_flow",
        external_dag_id="stock_cn_a_capital_flow",
        external_task_id="poll_capital_flow",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        timeout=2 * 60 * 60,
        poke_interval=60,
        mode="reschedule",
    )
    wait_northbound = ExternalTaskSensor(
        task_id="wait_northbound",
        external_dag_id="stock_cn_a_northbound",
        external_task_id="poll_northbound",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        timeout=2 * 60 * 60,
        poke_interval=60,
        mode="reschedule",
    )
    submit_aggregate, poll_aggregate = _submit_pipeline_fanout(
        "aggregate", "stock_cn_a_aggregate", timeout=60 * 60,
    )
    [wait_capital_flow, wait_northbound] >> submit_aggregate


# ===========================================================================
# Stock CN_A — Pack 1+2 envelope plans (W2 / W3.P1.2 / W3.P1.3 / W7)
#
# These DAGs feed CP envelope endpoint /api/internal/records/{kind} which
# AT (agent-toolkit) retrievers consume directly. Independent of the
# stock_cn_a_aggregate ε plan — different kind (records vs aggregate) and
# different downstream (Bobby retriever inject vs system_prompt inject).
#
# Schedule strategy (Asia/Shanghai, jydb timezone):
#   - dz_dailyquote (price_history)  → 23:30, jydb EOD ready ~22:00, gives
#     ~30min buffer; close enough to capital_flow 22:00 / aggregate 22:30
#     window so jydb pool burst stays in evening, not midnight.
#   - ps_newscontent / ps_newstag (news) → 02:00 cn_a / 02:15 hk, jydb
#     news ETL写入是 rolling, 凌晨拉一次拿全量 day. Frequency可后期改 hourly
#     若 user 抱怨实效 (codex review #5 提了 max_stale 问题).
#   - dz_balancesheetall + ... (financials) → 02:30 cn_a / 02:45 hk, 错开 news
#     避免 jydb pool concurrent peak.
#   - c_ex_targetprice (analyst_rating) / dz_dividend → 03:00 / 03:30, 事件型
#     稀疏, 错开 financials.
# All schedule windows finish before 06:00, in time for stock_daily 06:00
# enrichment + before 国内 user 早上 8:00 起来问问题。
#
# ─── stock_cn_a_news (daily 02:00) ─────────────────────────────────────

with DAG(
    dag_id="stock_cn_a_news",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="0 2 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily", "envelope"],
) as dag_cn_a_news:
    _submit_pipeline_fanout(
        "cn_a_news", "stock_cn_a_news", timeout=60 * 60,
    )


# ─── stock_cn_a_financials (daily 02:30) ───────────────────────────────

with DAG(
    dag_id="stock_cn_a_financials",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="30 2 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily", "envelope"],
) as dag_cn_a_financials:
    _submit_pipeline_fanout(
        "cn_a_financials", "stock_cn_a_financials", timeout=60 * 60,
    )


# ─── stock_cn_a_price_history (daily 23:30) ────────────────────────────

with DAG(
    dag_id="stock_cn_a_price_history",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="30 23 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily", "envelope"],
) as dag_cn_a_price_history:
    _submit_pipeline_fanout(
        "cn_a_price_history", "stock_cn_a_price_history", timeout=60 * 60,
    )


# ─── stock_cn_a_analyst_rating (daily 03:00) ───────────────────────────

with DAG(
    dag_id="stock_cn_a_analyst_rating",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="0 3 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily", "envelope"],
) as dag_cn_a_analyst_rating:
    _submit_pipeline_fanout(
        "cn_a_analyst_rating", "stock_cn_a_analyst_rating", timeout=60 * 60,
    )


# ─── stock_cn_a_dividend (daily 03:30) ─────────────────────────────────

with DAG(
    dag_id="stock_cn_a_dividend",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="30 3 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "cn_a", "daily", "envelope"],
) as dag_cn_a_dividend:
    _submit_pipeline_fanout(
        "cn_a_dividend", "stock_cn_a_dividend", timeout=60 * 60,
    )


# ===========================================================================
# Stock HK — W3.P1.3.C2 / W5 / W6 plans
#
# HK plans run jydb 同源 tables but via hk_secumain (not secumain). Per-ticker
# fan-out same as cn_a; ticker_limit=6000 cap covers HK ~2000 active universe.
#
# hk_master MUST exist before per-ticker HK fan-outs (submit-pipeline pulls
# from hk master snapshot). First-time deploy order: enable hk_master DAG +
# trigger one manual run; once hk master snapshot is written, enable other
# HK DAGs. After that, monthly hk_master refresh + daily plans run normally.

# ─── stock_hk_master (monthly Sun 02:30) ───────────────────────────────

with DAG(
    dag_id="stock_hk_master",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    # 1st Sunday of each month at 02:30 — 30min after cn_a_master 02:00
    # to spread jydb pool peak (both fetch full secumain table)
    schedule_interval="30 2 1-7 * 0",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "hk", "monthly"],
) as dag_hk_master:
    submit_hk_master = SimpleHttpOperator(
        task_id="submit_hk_master",
        method="POST",
        http_conn_id="content-platform",
        endpoint="/api/runs",
        headers={**_AUTH_HEADERS, "Idempotency-Key": _IDEM_KEY},
        data=json.dumps({
            "plan_id": "stock_hk_master",
            "input_data": {"as_of_date": "{{ ds }}"},
        }),
        response_check=lambda r: r.status_code in (200, 201),
        response_filter=lambda r: r.json()["run_id"],
        log_response=True,
    )
    poll_hk_master = HttpSensor(
        task_id="poll_hk_master",
        http_conn_id="content-platform",
        endpoint=(
            "/api/runs/{{ task_instance.xcom_pull(task_ids='submit_hk_master') }}"
        ),
        headers=_AUTH_HEADERS,
        response_check=_check_run_terminal,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    submit_hk_master >> poll_hk_master


# ─── stock_hk_southbound (daily 18:45) ─────────────────────────────────
#
# 港股通南向 (HK 那一档) holdings — 跟 cn_a_northbound 18:30 同源不同方向, 错开 15min.

with DAG(
    dag_id="stock_hk_southbound",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="45 18 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "hk", "daily", "envelope"],
) as dag_hk_southbound:
    wait_master = _wait_hk_master_published()
    submit, poll = _submit_pipeline_fanout(
        "hk_southbound", "stock_hk_southbound", timeout=60 * 60,
    )
    wait_master >> submit


# ─── stock_hk_news (daily 02:15) ───────────────────────────────────────

with DAG(
    dag_id="stock_hk_news",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="15 2 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "hk", "daily", "envelope"],
) as dag_hk_news:
    wait_master = _wait_hk_master_published()
    submit, poll = _submit_pipeline_fanout(
        "hk_news", "stock_hk_news", timeout=60 * 60,
    )
    wait_master >> submit


# ─── stock_hk_financials (daily 02:45) ─────────────────────────────────

with DAG(
    dag_id="stock_hk_financials",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="45 2 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "hk", "daily", "envelope"],
) as dag_hk_financials:
    wait_master = _wait_hk_master_published()
    submit, poll = _submit_pipeline_fanout(
        "hk_financials", "stock_hk_financials", timeout=60 * 60,
    )
    wait_master >> submit


# ─── stock_us_financials (daily 14:00, W6 slice 2) ─────────────────────
#
# US 收盘 ~ 04:00 UTC = 12:00 Asia/Shanghai;FMP filing 入库 buffer ~2h.
# 14:00 跑 catches 同日 quarterly filings + revisions while staying out
# of the 凌晨 cn_a/hk envelope DAG window (avoid FMP rate-limit overlap).
#
# ticker_limit=1000 aligns with stock_daily / SEO universe (top by dollar
# volume); FMP cost ~3000 calls/day vs hk's 6000/day at 02:45.
#
# is_paused_upon_creation=True per W6 slice 2 ship gate (proposal v4
# Phase 2 unpause checklist) — owner manually unpauses after C2.1
# dry-run pilot (manual POST /submit-pipeline tickers=["AAPL"]) verifies
# envelope shape end-to-end. Unlike hk, no master-snapshot prereq;
# submit-pipeline endpoint pulls top-1000 directly via fmp_client.

with DAG(
    dag_id="stock_us_financials",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 9, tz="Asia/Shanghai"),
    schedule_interval="0 14 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "us", "daily", "envelope"],
    is_paused_upon_creation=True,
) as dag_us_financials:
    _submit_pipeline_fanout(
        "us_financials",
        "stock_us_financials",
        ticker_limit=1000,
        timeout=60 * 60,
    )


# ─── stock_hk_price_history (daily 23:45) ──────────────────────────────
#
# 港股 16:00 收盘 (vs cn_a 15:00), jydb 港股 EOD 入库时间约 22:00-23:00.
# 23:45 跟 cn_a_price_history 23:30 错开 15min, jydb pool 友好。

with DAG(
    dag_id="stock_hk_price_history",
    catchup=False,
    start_date=pendulum.datetime(2026, 5, 7, tz="Asia/Shanghai"),
    schedule_interval="45 23 * * *",
    max_active_runs=1,
    default_args=_VENDOR_PLAN_DEFAULT_ARGS,
    tags=["stock", "content-platform", "hk", "daily", "envelope"],
) as dag_hk_price_history:
    wait_master = _wait_hk_master_published()
    submit, poll = _submit_pipeline_fanout(
        "hk_price_history", "stock_hk_price_history", timeout=60 * 60,
    )
    wait_master >> submit
