from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

DAG_ID = "test_fetch_news"

with DAG(
        DAG_ID,
        catchup=False,
        start_date=datetime(2022, 1, 1),
        schedule_interval=timedelta(hours=8),
        default_args={
            "owner": "xiangpingjiang",
            "depends_on_past": False,
            "retries": 12,
            "retry_delay": timedelta(minutes=1),
        }
) as symbol_dag:

    fetch_news = KubernetesPodOperator(
        task_id="test______fetch_news",
        name="fetch-news",
        namespace="sim",
        image="rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:630dd2137aac0e2a42ca128faf6fb1265432895f",

        cmds=["python"],
        arguments=["-m", "jobs.news"],

        get_logs=True,
        is_delete_operator_pod=True,
    )