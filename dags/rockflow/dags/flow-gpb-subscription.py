import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# flow 环境（namespace sim）商店渠道（Google Play Billing）对账：一个接口串三件事，
# 优先级从高到低——
#
# 1. **补 ack（3 天窗口）**。Play 规定购买后 3 天内不确认就自动退款。正常路径是 verify 落权益
#    后当场 ack，但 ack 调用失败、或 RTDN 没送达时没人补这一刀——权益已经给了、3 天后钱被退。
#    这条是资损方向，本 DAG 存在的首要理由。
# 2. **收尾放弃支付的意向单**。用户关掉 Play 支付窗口时 Google 不发任何通知，服务端手上只剩
#    一张 PENDING 单。超过 flow.order.gpb.reconcile.abandon-after-minutes（默认 1440）才作废。
# 3. **补漏的续费/取消**。RTDN 没送到时按 token 向 Google 逐条回查补齐。
#
# 服务端有 Redisson 锁且按 token/orderId 幂等，重试与并发均安全。
# 首次启用前先手工跑一次 dryRun=true，确认扫出来的确实是该修的那批。
flow_subscription_reconcile_gpb = DAG(
    "flow_subscription_reconcile_gpb",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 29, tz='Asia/Hong_Kong'),
    # 服务端有 30 分钟宽限期，每小时一次足够。
    schedule_interval='55 * * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        # 对账幂等可以重试，但每次重试都要重新烧 Google Developer API 的配额。
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='flow_subscription_reconcile_gpb',
    method='POST',
    http_conn_id='flow-order',
    endpoint='/order/inner/subscription/reconcile-gpb?limit=200&dryRun=false',
    response_check=lambda response: response.json()['code'] == 200,
    # 每条命中的订阅都要向 Google 查一次当前状态，200 条按最坏情况给足。
    extra_options={"timeout": 900},
    dag=flow_subscription_reconcile_gpb,
)


# flow 环境商店渠道（Google Play Billing）退款补偿扫描：把 Google 那边作废了、而 RTDN 没送到的
# 退款补回业务链路。
#
# **只补退款**：续费、取消、过期这些状态上面那条对账按 token 回查就能补齐；退款却是
# 「订阅看起来仍正常、钱已经退了」——从订阅状态看不出来，只能去 voidedpurchases 列表里捞。
#
# 扫描窗口有硬上限：服务端 GooglePlayNotificationSweeper.MAX_WINDOW_MINUTES = 3 天，超过直接
# 抛参数异常。重放共用 router 那把按 orderId 的 Redis 幂等键（payment.callback.expire.day，
# 默认 7 天）——窗口逼近 TTL 时，早于 TTL 的退款会被当成没见过再处理一遍，补偿任务就成了
# 重复退款的来源。24 小时窗口 / 6 小时一跑 = 4 倍重叠，离 3 天上限也留足余量。
#
# 结果里 skipped 高是正常的；**routed > 0 才是「救回来一条」**，服务端同时打
# [gpb][sweep] recovered a voided purchase 的 warn。
#
# ⚠️ 前置：Apollo flow.payment.google-play.service-account.* 必须配齐，没配时出站能力整组
#    不注册，接口会抛 IllegalStateException（cannot sweep）。
flow_notification_sweep_gpb = DAG(
    "flow_notification_sweep_gpb",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 29, tz='Asia/Hong_Kong'),
    schedule_interval='40 */6 * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        # 重放幂等可以重试；但每次重试都要把整个窗口向 Google 重拉一遍，别给太多次。
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    }
)

SimpleHttpOperator(
    task_id='flow_notification_sweep_gpb',
    method='POST',
    http_conn_id='flow-payment',
    # minutes 必须 <= 3 天（服务端硬校验），且要显著小于 router 幂等键的 TTL（见上）。
    endpoint='/payment/inner/google-play/notifications/sweep?minutes=1440',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 900},
    dag=flow_notification_sweep_gpb,
)
