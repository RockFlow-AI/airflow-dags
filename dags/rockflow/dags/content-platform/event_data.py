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

IMAGE = "rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:820e73a9f9814706061922d5b04ec44e0483a8f9"

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
        params=params or {"bobbyUserId": "7472495549074249214"},
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
    "big_mover_price_us",
    ["jobs/event_data_source/big_mover_price_us.py"],
)


make_dag(
    "big_mover_volume_us",
    ["jobs/event_data_source/big_mover_volume_us.py"],
)
