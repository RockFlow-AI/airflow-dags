from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.mic import *

with DAG("mic_download", default_args=DEFAULT_DEBUG_ARGS) as mic:
    MicDownloadOperator(
        key=mic.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
