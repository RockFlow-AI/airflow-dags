import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Bobby 商店渠道（Google Play Billing）对账：一个接口里串了三件事，优先级从高到低——
#
# 1. **补 ack（3 天窗口）**。Play 规定购买后 3 天内不确认（acknowledge/consume）就自动退款。
#    正常路径是 verify 落权益后服务端当场 ack，但 ack 调用失败、或 RTDN 根本没送达时没人补这一刀
#    ——后果是权益已经给了、3 天后 Google 把钱退了。这条是资损方向，本 DAG 存在的首要理由。
# 2. **收尾放弃支付的意向单**。用户点了订阅/购买又关掉 Play 的支付窗口时，Google 不会发任何
#    通知，客户端也不会调 verify——服务端手上只剩一张发出去的 PENDING 单，无从得知它已经死了。
#    超过 flow.order.gpb.reconcile.abandon-after-minutes（默认 1440）才作废，期间用户账单里
#    挂的是"处理中"。作废后慢速支付（现金等）完成仍可复活，所以这个数只决定"处理中"挂多久。
# 3. **补漏的续费/取消**。RTDN 没送到时按 token 向 Google 逐条回查补齐。
#
# 服务端有 Redisson 锁（同一时刻只跑一轮，重入直接返回空结果）且按 token/orderId 幂等，
# 重试与并发均安全。limit 是单轮扫描上限，扫不完的留给下一轮。
#
# 首次启用前先手工跑一次 dryRun=true，确认扫出来的确实是该修的那批，再让本 DAG 接管。
#
# 返回体 result 字段：acknowledged（补了几刀 ack）/ abandoned（作废了几张意向单）/
# scanned / repaired（补了几笔续费）/ flagged（需人工）/ failed。
bobby_subscription_reconcile_gpb = DAG(
    "bobby_subscription_reconcile_gpb",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 20, tz='Asia/Hong_Kong'),
    # 服务端有 30 分钟宽限期（避开「周期末刚过、续费通知还在路上」的边界），每小时跑一次足够。
    # 错开 45 分：0 分是结算、30 分是 IAP 对账，三个任务没必要挤在一起打 order。
    schedule_interval='45 * * * *',
    max_active_runs=1,
    default_args={
        "owner": "guohongtao",
        "depends_on_past": False,
        # 对账幂等可以重试，但每次重试都要重新烧 Google Developer API 的配额；
        # 扫不完的下一轮自然会再来。
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='bobby_subscription_reconcile_gpb',
    method='POST',
    http_conn_id='flow-order.bobby',
    endpoint='/order/inner/subscription/reconcile-gpb?limit=200&dryRun=false',
    response_check=lambda response: response.json()['code'] == 200,
    # 每条命中的订阅都要向 Google 查一次当前状态，200 条按最坏情况给足。
    extra_options={"timeout": 900},
    dag=bobby_subscription_reconcile_gpb,
)


# Bobby 商店渠道（Google Play Billing）退款补偿扫描：把 Google 那边作废了、而 RTDN 没送到的
# 退款补回业务链路。
#
# **只补退款**，这一点和 Apple 那条通知历史补偿不同：续费、取消、过期这些状态，上面那条对账
# 任务按 token 逐条回查就能补齐；退款却是"订阅看起来仍正常、钱已经退了"——从订阅状态上
# 根本看不出来，只能单独去 voidedpurchases 列表里捞。
#
# 扫描窗口有硬上限：服务端 GooglePlayNotificationSweeper.MAX_WINDOW_MINUTES = 3 天，
# 超过直接抛参数异常。理由是重放共用 router 那把按 orderId 的 Redis 幂等键
# （payment.callback.expire.day，默认 7 天）——窗口一旦逼近 TTL，早于 TTL 的退款会被当成
# 没见过再处理一遍，补偿任务就成了重复退款的来源。24 小时窗口 / 6 小时一跑 = 4 倍重叠，
# 离 3 天的上限也留足余量。改这两个数之前先一起看那个配置。
#
# 结果里 skipped 高是正常的（绝大多数退款本来就由 RTDN 收到了）；**routed > 0 才是「救回来
# 一条」**，服务端会同时打一条 warn 级日志（[gpb][sweep] recovered a voided purchase），
# 告警按它配。
#
# ⚠️ 两个前置，缺一个这个 DAG 就跑不起来：
#    1. Airflow 连接 flow-payment.bobby —— 现有 DAG 都打 flow-order.bobby，payment 侧的连接
#       要单独建。Apple 那条 sweep DAG 上线时就留了同一条警告。
#    2. Apollo flow.payment.google-play.service-account.* —— 没配时出站能力整组不注册，
#       接口会抛 IllegalStateException（cannot sweep）。
bobby_notification_sweep_gpb = DAG(
    "bobby_notification_sweep_gpb",
    catchup=False,
    start_date=pendulum.datetime(2026, 10, 20, tz='Asia/Hong_Kong'),
    # 错开 50 分：0 分结算、15 分 IAP sweep、30 分 IAP 对账、45 分 GPB 对账。
    schedule_interval='50 */6 * * *',
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
    task_id='bobby_notification_sweep_gpb',
    method='POST',
    http_conn_id='flow-payment.bobby',
    # minutes 必须 <= 3 天（服务端硬校验），且要显著小于 router 幂等键的 TTL（见上）。
    endpoint='/payment/inner/google-play/notifications/sweep?minutes=1440',
    response_check=lambda response: response.json()['code'] == 200,
    # 一个 24 小时窗口可能要翻多页，每页都是一次 Google 调用；按最坏情况给足。
    extra_options={"timeout": 900},
    dag=bobby_notification_sweep_gpb,
)
