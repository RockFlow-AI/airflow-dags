from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

secret_file = Secret(
    deploy_type="volume",
    deploy_target="/root/.ssh",
    secret="prod-ssh-secret",
)

IMAGE = "rockflow-registry-vpc.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:0849cf8e570394011f0f3e361bd003ea7a4b9ff8"

DEFAULT_ARGS = {
    "owner": "xiangpingjiang",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def make_dag(dag_id, arguments, params=None):
    with DAG(
        dag_id,
        tags=["content-platform-airflow"],
        catchup=False,
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        params=params,
    ):
        KubernetesPodOperator(
            task_id=dag_id,
            name=dag_id.replace("_", "-"),
            namespace="prod",
            image=IMAGE,
            cmds=["python"],
            arguments=arguments,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "512Mi"},
                limits={"cpu": "1", "memory": "1Gi"},
            ),
            env_vars=[
                k8s.V1EnvVar(name="NAMESPACE", value="prod"),
            ],
            get_logs=True,
            is_delete_operator_pod=True,
            secrets=[secret_file],
        )


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
)

make_dag(
    "translate_daily_generate",
    ["jobs/content_generate.py/translate_daily_generate.py"],
)