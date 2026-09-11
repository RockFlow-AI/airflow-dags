import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Bobby 商店渠道（Apple IAP）续费对账：把 Apple 已经扣过、我们却没收到通知的续费补记进来。
#
# 存在的理由是 ASSN 没有投递保证 —— 2026-09-11 SIT 上一条订阅的首购通知整条丢失（端点连一次
# 入站记录都没有，Apple 之后也没重推），而客户端不在线时（卸载、长期不开 App）verify 腿同样
# 补不上。这时只能我们主动去问 Apple。
#
# 只补记漏掉的扣款，绝不撤权益、不判终态 —— 那是不可逆的资损方向。Apple 说已过期或不认识这条
# 订阅时服务端只告警，等真事件或人工。
#
# 接口幂等（同一笔扣款按 Apple 的 transactionId 判重）且服务端有 Redisson 锁，重试与并发均安全。
#
# 首次启用前先手工跑一次 dryRun=true，在日志里确认 [iap][reconcile] dry-run would repair 扫出来
# 的确实是该修的那批，再让本 DAG 接管。
bobby_subscription_reconcile_iap = DAG(
    "bobby_subscription_reconcile_iap",
    catchup=False,
    start_date=pendulum.datetime(2026, 9, 11, tz='Asia/Hong_Kong'),
    # 服务端有 30 分钟宽限期（避开「周期末刚过、续费通知还在路上」的边界），每小时跑一次足够。
    # 错开整点：结算 DAG 占了 0 分，两个任务同时打 order 没有必要。
    schedule_interval='30 * * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        # 对账幂等可以重试，但每次重试都要重新烧 Apple 的调用配额；扫不完的下一轮自然会再来。
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='bobby_subscription_reconcile_iap',
    method='POST',
    http_conn_id='flow-order.bobby',
    # limit 是单轮扫描上限，扫不完的留给下一轮。
    endpoint='/order/inner/subscription/reconcile-iap?limit=200&dryRun=false',
    response_check=lambda response: response.json()['code'] == 200,
    # 每条命中的订阅都要向 Apple 查一次当前状态，200 条按最坏情况给足。
    extra_options={"timeout": 900},
    dag=bobby_subscription_reconcile_iap,
)
