import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# flow 环境（namespace sim）商店渠道（Apple IAP）续费对账：把 Apple 已经扣过、我们却没收到
# 通知的续费补记进来。ASSN 没有投递保证（SIT 上出现过首购通知整条丢失、Apple 也不重推），
# 客户端不在线时 verify 腿同样补不上，这时只能主动去问 Apple。
#
# 只补记漏掉的扣款，绝不撤权益、不判终态 —— 那是不可逆的资损方向。
# 同一轮里顺带收尾「放弃支付的下单意向」，默认超过 24 小时才作废
# （flow.order.iap.reconcile.abandon-after-minutes），给 Ask to Buy 的家长审批留足时间。
#
# 接口幂等（按 Apple 的 transactionId 判重）且服务端有 Redisson 锁，重试与并发均安全。
# 首次启用前先手工跑一次 dryRun=true，确认 [iap][reconcile] dry-run would repair 扫出来的
# 确实是该修的那批。
flow_subscription_reconcile_iap = DAG(
    "flow_subscription_reconcile_iap",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 29, tz='Asia/Hong_Kong'),
    # 服务端有 30 分钟宽限期（避开「周期末刚过、续费通知还在路上」的边界），每小时一次足够。
    schedule_interval='35 * * * *',
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
    task_id='flow_subscription_reconcile_iap',
    method='POST',
    http_conn_id='flow-order',
    endpoint='/order/inner/subscription/reconcile-iap?limit=200&dryRun=false',
    response_check=lambda response: response.json()['code'] == 200,
    # 每条命中的订阅都要向 Apple 查一次当前状态，200 条按最坏情况给足。
    extra_options={"timeout": 900},
    dag=flow_subscription_reconcile_iap,
)


# flow 环境商店渠道（Apple IAP）通知历史补偿扫描：把 Apple 那边有、而我们从没处理过的通知补回来。
#
# 它补的是上面那条对账 **结构上够不着** 的那一块：对账只扫「本地周期已过 + 状态仍生效 +
# 渠道订阅号非空」的订阅行；首购通知整条丢失时那条订阅对对账是不存在的，后果是用户付了钱
# 却没有权益。
#
# 重放走与 webhook 相同的路（验签 → router），重复由 payment 侧那把 Redis 幂等键挡住。
# **扫描窗口必须显著小于该键的 TTL**（payment.callback.expire.day，默认 7 天）：窗口一旦超过
# TTL，早于 TTL 的通知会被当成没见过再处理一遍，补偿任务就成了重复扣账的来源。
# 24 小时窗口 / 6 小时一跑 = 4 倍重叠，离 7 天上限留足余量。改这两个数之前先一起看那个配置。
#
# 结果里 skipped 高是正常的；**routed > 0 才是「救回来一条」**，服务端同时打 warn 级日志。
flow_notification_sweep_iap = DAG(
    "flow_notification_sweep_iap",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 29, tz='Asia/Hong_Kong'),
    schedule_interval='20 */6 * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        # 重放幂等可以重试；但每次重试都要重新把整个窗口向 Apple 拉一遍，别给太多次。
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    }
)

SimpleHttpOperator(
    task_id='flow_notification_sweep_iap',
    method='POST',
    http_conn_id='flow-payment',
    # minutes 是回捞窗口，必须 < payment 侧幂等键的 TTL（见上）。
    endpoint='/payment/inner/apple/notifications/sweep?minutes=1440',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 900},
    dag=flow_notification_sweep_iap,
)
