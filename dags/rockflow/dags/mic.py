from airflow.models import DAG

from rockflow.dags.const import DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS
from rockflow.operators.mic import MicDownloadOperator

with DAG("symbol_download", default_args=DEFAULT_DEBUG_ARGS) as dag:
    nasdaq = MicDownloadOperator(
        key="nasdaq_raw_key",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
