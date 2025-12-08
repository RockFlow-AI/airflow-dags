from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "tradebot_live_trade",
    catchup=False,
    start_date=pendulum.datetime(2025, 12, 08),
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
        data={
            "disable_trades": False,        # True 时只做演练不下单
            "max_arenas": None,             # 可设为整数限制数量
            "dataset_prefix": None,         # 可选
            "filter_arena_ids": None,       # "id1,id2" 或 None
            "filter_participant_ids": None, # "uid1,uid2" 或 None
        },
        response_check=lambda response: response.json().get("code") == 200,
    )
