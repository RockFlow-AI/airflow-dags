from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "tradebot_live_trade",
    catchup=False,
    start_date=pendulum.datetime(2025, 11, 8),
    schedule_interval="*/5 * * * *",
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 0,
    },
) as trade_bot_arena_trade:
    tradebot_live_task = SimpleHttpOperator(
        task_id="tradebot_live_trade",
        method="POST",
        http_conn_id="tradebot",
        endpoint="/bot/api/arena/live/run",  # 注意前缀
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "disable_trades": False,
            "max_arenas": None,
            "dataset_prefix": None,
            "filter_arena_ids": None,
            "filter_participant_ids": None,
        }),
        response_check=lambda response: response.json().get("code") == 200,
    )
