from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

secret_file = Secret(
    deploy_type="volume",
    deploy_target="/root/.ssh",
    secret="devpod-ssh-secret",
)

IMAGE = "rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:e470cfdcb2856e14d347a86283bd563160e59f55"

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
            namespace="airflow",
            image=IMAGE,
            cmds=["python"],
            arguments=arguments,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "512Mi"},
                limits={"cpu": "1", "memory": "1Gi"},
            ),
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
