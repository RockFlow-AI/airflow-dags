from airflow.models import DAG

from rockflow.dags.const import MERGE_CSV_KEY, DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS
from rockflow.operators.futu import *

with DAG("company_profile_batch_download", default_args=DEFAULT_DEBUG_ARGS) as batch_dag:
    FutuBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=batch_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_debug", default_args=DEFAULT_DEBUG_ARGS) as batch_dag_debug:
    FutuBatchOperatorDebug(
        from_key=MERGE_CSV_KEY,
        key=batch_dag_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract", default_args=DEFAULT_DEBUG_ARGS) as extract_dag:
    FutuExtractHtml(
        from_key="company_profile_batch_download/",
        key=extract_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract_debug", default_args=DEFAULT_DEBUG_ARGS) as extract_dag_debug:
    FutuExtractHtmlDebug(
        from_key="company_profile_batch_download/",
        key=extract_dag_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
