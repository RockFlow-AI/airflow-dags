from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    "arena_pnl_guard",  #以此区分不同的 DAG ID
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 23),
    schedule_interval='*/5 * * * *',  # 设置为每5分钟执行一次
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
) as arena_pnl_guard:

    arena_pnl_guard_task = SimpleHttpOperator(
        task_id='arena_pnl_guard',
        method='POST',  # 根据 curl -X POST
        http_conn_id='rockbot',  # 假设你复用相同的 host 配置
        # 注意：这里去掉了 host 部分，只保留 path。
        # 如果 'rockbot' conn_id 配置的 host 不是 10.130.10.76:3079，你需要新建一个 connection 或写完整 url
        endpoint='/bot/api/arena/v2/arena_v2/pnl_guard',
        response_check=lambda response: response.json().get('code') == 200,
    )