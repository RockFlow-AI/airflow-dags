"""
Entity Index 同步定时任务

功能：定时从后端数据库同步最新股票列表并更新到 Elasticsearch
调度：每小时执行一次
"""

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum


# DAG 配置
with DAG(
    "entity_index_sync_hourly",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Shanghai'),
    schedule_interval='0 * * * *',  # 每小时执行一次
    default_args={
        "owner": "rockbot",
        "depends_on_past": False,
        "retries": 2,  # 失败重试 2 次
    }
) as entity_index_sync_hourly:
    
    # 同步 entity index 任务
    sync_task = SimpleHttpOperator(
        task_id='entity_index_sync_hourly',
        method='POST',
        http_conn_id='rockbot',
        endpoint='/bot/api/index/entity/sync',
        response_check=lambda response: response.json()['code'] == 200,
        dag=entity_index_sync_hourly
    )

