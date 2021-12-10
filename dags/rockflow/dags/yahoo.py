from airflow.models import DAG

from rockflow.dags.const import DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS, MERGE_CSV_KEY
from rockflow.operators.yahoo import YahooBatchOperator

with DAG("yahoo_download", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download:
    YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("yahoo_download_debug", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download_debug:
    YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
