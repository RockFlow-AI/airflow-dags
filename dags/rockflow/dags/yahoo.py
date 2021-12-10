from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.yahoo import *

with DAG("yahoo_download", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download:
    YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("yahoo_download_debug", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download_debug:
    YahooBatchOperatorDebug(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
