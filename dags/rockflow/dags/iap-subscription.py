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


# Bobby 商店渠道（Apple IAP）通知历史补偿扫描：把 Apple 那边有、而我们从没处理过的通知补回来。
#
# 它补的是上面那条对账 **结构上够不着** 的那一块。对账只扫「本地周期已过 + 状态仍生效 +
# 渠道订阅号非空」的订阅行；首购通知整条丢失时，订阅行根本没落、或落了没有渠道订阅号 ——
# 那条订阅对对账是不存在的，后果是用户付了钱却没有权益。
#
# 重放走的是与 webhook 完全相同的那条路（验签 → router），重复由 payment 侧那把 Redis 幂等键
# 挡住。**因此扫描窗口必须显著小于该键的 TTL**（payment.callback.expire.day，默认 7 天）：
# 窗口一旦超过 TTL，早于 TTL 的通知会被当成没见过再处理一遍，补偿任务就成了重复扣账的来源。
# 24 小时窗口 / 6 小时一跑 = 4 倍重叠，离 7 天上限留足余量。改这两个数之前先一起看那个配置。
#
# 结果里 skipped 高是正常的（绝大多数通知本来就由 webhook 收到了）；**routed > 0 才是「救回来
# 一条」**，服务端会同时打一条 warn 级日志，告警按它配。
#
# ⚠️ 首次上线前要先建好 Airflow 连接 flow-payment.bobby —— 现有 DAG 都打 flow-order.bobby，
#    payment 还没有连接。没建的话这个 DAG 会直接连不上。
bobby_notification_sweep_iap = DAG(
    "bobby_notification_sweep_iap",
    catchup=False,
    start_date=pendulum.datetime(2026, 9, 14, tz='Asia/Hong_Kong'),
    # 错开 15 分：整点是结算、30 分是对账，三个任务没必要挤在一起。
    schedule_interval='15 */6 * * *',
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
    task_id='bobby_notification_sweep_iap',
    method='POST',
    http_conn_id='flow-payment.bobby',
    # minutes 是回捞窗口，必须 < payment 侧幂等键的 TTL（见上）。
    endpoint='/payment/inner/apple/notifications/sweep?minutes=1440',
    response_check=lambda response: response.json()['code'] == 200,
    # 一个 24 小时窗口可能要翻多页，每页都是一次 Apple 调用；按最坏情况给足。
    extra_options={"timeout": 900},
    dag=bobby_notification_sweep_iap,
)
