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

DAG_ID = "single_daily_report_and_positions_card_generate"

with DAG(
        DAG_ID,
        tags=["content-platform-airflow"],
        catchup=False,
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            "owner": "xiangpingjiang",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
        },
        params={
            "bobbyUserId": "7472495549074249214",
        },
) as symbol_dag:

    daily_report_and_positions_card_generate = KubernetesPodOperator(
        task_id="daily_report_and_positions_card_generate",
        name="single-daily-report-and-positions-card-generate",
        namespace="airflow",
        image="rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:820e73a9f9814706061922d5b04ec44e0483a8f9",

        cmds=["python"],
        arguments=["jobs/content_generate.py/single_positions.py", "--bobbyUserId", "{{ params.bobbyUserId }}"],
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