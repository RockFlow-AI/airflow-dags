from datetime import datetime, timedelta

from airflow.models import DAG

from rockflow.dags.const import DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME
from rockflow.operators.mic import MicDownloadOperator

default_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    # "start_date": datetime(2021, 12, 8),
    "start_date": datetime.now(),
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@once",  # for debug
    # "schedule_interval": "0 */12 * * *",
}

with DAG("symbol_download", default_args=default_args) as dag:
    nasdaq = MicDownloadOperator(
        key="nasdaq_raw_key",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
