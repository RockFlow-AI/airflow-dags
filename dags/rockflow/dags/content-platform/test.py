from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.secret import Secret
secret_file = Secret(
    deploy_type="volume",
    deploy_target="/root/.ssh",
    secret="devpod-ssh-secret",
)

DAG_ID = "test_fetch_news"

with DAG(
        DAG_ID,
        catchup=False,
        start_date=datetime(2022, 1, 1),
        schedule_interval=timedelta(hours=8),
        default_args={
            "owner": "xiangpingjiang",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
        }
) as symbol_dag:

    fetch_news = KubernetesPodOperator(
        task_id="test______fetch_news",
        name="fetch-news-debug-airflow",
        namespace="airflow",
        image="rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:8c494a443951e227ff9a0d87a8ba83a4074e2904",

        cmds=["python"],
        arguments=["jobs/content_generate.py/single_personal.py", "--bobbyUserId","7472495549074249214"],
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "500m",
                "memory": "512Mi",
            },
            limits={
                "cpu": "1",
                "memory": "1Gi",
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        secrets=[secret_file],
    )