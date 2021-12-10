from airflow.models import DAG

from rockflow.dags.const import DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS
from rockflow.operators.mic import MicDownloadOperator

with DAG("mic_download", default_args=DEFAULT_DEBUG_ARGS) as dag:
    MicDownloadOperator(
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
