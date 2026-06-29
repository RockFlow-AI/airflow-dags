from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

import os


# 依赖数据库连接域名判断环境
_db_conn = os.environ.get("AIRFLOW_CONN_AIRFLOW_DB", "")
ENV = "prod" if "rds-prod.rds.rockflow.ai" in _db_conn else "airflow"

secret_file = Secret(
    deploy_type="volume",
    deploy_target="/root/.ssh",
    secret="prod-ssh-secret" if ENV == "prod" else "devpod-ssh-secret",
)
image_tag = "58e29f1107696fb43128501704b175290e484261"
IMAGES = {
    "prod": f"rockflow-registry-vpc.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:{image_tag}",
    "airflow":  f"rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:{image_tag}",
}
IMAGE = IMAGES[ENV]
DEFAULT_ARGS = {
    "owner": "xiangpingjiang",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def make_dag(dag_id, arguments, params=None, schedule_interval=None, timezone="UTC"):
    with DAG(
        dag_id,
        tags=["content-platform-airflow-data"],
        catchup=False,
        start_date=pendulum.datetime(2022, 1, 1, tz=timezone),
        schedule_interval=schedule_interval,
        default_args=DEFAULT_ARGS,
        params=params,
    ):
        KubernetesPodOperator(
            task_id=dag_id,
            name=dag_id.replace("_", "-"),
            namespace=ENV,
            image=IMAGE,
            cmds=["python"],
            arguments=arguments,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "512Mi"},
                limits={"cpu": "1", "memory": "1Gi"},
            ),
            env_vars=[k8s.V1EnvVar(name="NAMESPACE", value=ENV)] if ENV == "prod" else [],
            get_logs=True,
            is_delete_operator_pod=True,
            secrets=[secret_file],
        )

##### data
make_dag(
    "big_mover_price_us",
    ["jobs/event_data_source/big_mover_price_us.py"],
    schedule_interval="30 17 * * *",
    timezone="America/New_York",
)


make_dag(
    "big_mover_volume_us",
    ["jobs/event_data_source/big_mover_volume_us.py"],
    schedule_interval="30 17 * * *",
    timezone="America/New_York",
)


make_dag(
    "dividends_calendar",
    ["jobs/event_data_source/dividends_calendar.py"],
    schedule_interval="00 18 * * *",
    timezone="America/New_York",
)



make_dag(
    "splits_calendar",
    ["jobs/event_data_source/splits_calendar.py"],
    schedule_interval="02 18 * * *",
    timezone="America/New_York",
)



make_dag(
    "earnings_calendar",
    ["jobs/event_data_source/earnings_calendar.py"],
    schedule_interval="05 18 * * *",
    timezone="America/New_York",
)

make_dag(
    "ark",
    ["jobs/top_voices_movements/ark.py"],
    schedule_interval="00 19 * * *",
    timezone="America/New_York",
)

make_dag(
    "clear_events",
    ["jobs/event_data_source/clear_events.py", "--date", "{{ params.date }}"],
)










######## generate

make_dag(
    "single_education_card_generate",
    ["jobs/content_generate.py/single_investor_education.py", "--bobbyUserId", "{{ params.bobbyUserId }}"],
    params={"bobbyUserId": "7472495549074249214"},
)


make_dag(
    "single_daily_report_and_positions_card_generate",
    ["jobs/content_generate.py/single_positions.py", "--bobbyUserId", "{{ params.bobbyUserId }}"],
    params={"bobbyUserId": "7472495549074249214"},
)

make_dag(
    "single_translate",
    ["jobs/content_generate.py/single_translate.py", "--bobbyUserId", "{{ params.bobbyUserId }}"],
    params={"bobbyUserId": "7472495549074249214"},
)

make_dag(
    "positions_card_daily_generate",
    ["jobs/content_generate.py/positions_card_daily_generate.py"],
    schedule_interval="00 20 * * *",
    timezone="America/New_York",
)


make_dag(
    "investor_education_generate",
    ["jobs/content_generate.py/investor_education_generate.py"],
    schedule_interval="00 21 * * 3,6",
    timezone="America/New_York",

)

make_dag(
    "translate_daily_generate",
    ["jobs/content_generate.py/translate_daily_generate.py"],
    schedule_interval="00 22 * * *",
    timezone="America/New_York",
)

make_dag(
    "clear_cards",
    ["jobs/content_generate.py/clear_cards.py", "--date", "{{ params.date }}","--user_id","{{ params.bobbyUserId }}"],
)