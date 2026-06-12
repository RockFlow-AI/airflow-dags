from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


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
        namespace="sim",
        image="rockflow-registry-vpc.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:be1bce6dab0e97aaa1093d95f971271e3860839f",

        cmds=["python"],
        arguments=["-m", "jobs.news"],
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
    )