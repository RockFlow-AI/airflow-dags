import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Bobby 待取消订阅到期结算：把周期末已过、仍停在待取消的订阅翻成终态并下掉 VIP。
# Waffo 取消后不再推周期末通知，这是「待取消 → 已取消」唯一的驱动源。
# 接口幂等（重复调只会 settled=0）且服务端有 Redisson 锁，重试与并发均安全。
bobby_subscription_settle_expired = DAG(
    "bobby_subscription_settle_expired",
    catchup=False,
    start_date=pendulum.datetime(2026, 8, 10, tz='Asia/Hong_Kong'),
    # 服务端有 2 小时宽限期（避开与在途续费/取消 webhook 的边界竞态），每小时跑一次足够。
    schedule_interval='0 * * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='bobby_subscription_settle_expired',
    method='POST',
    http_conn_id='flow-order.bobby',
    # limit 是单轮扫描上限，扫不完的留给下一轮。
    endpoint='/order/inner/subscription/settle-expired?limit=500&dryRun=false',
    response_check=lambda response: response.json()['code'] == 200,
    # 单轮最多扫 500 条，其中恢复过的订阅每条还要回查一次 Waffo，超时给足。
    extra_options={"timeout": 600},
    dag=bobby_subscription_settle_expired,
)
