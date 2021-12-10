from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.futu import *

with DAG("company_profile_batch_download", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_download:
    FutuBatchOperatorCn(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuBatchOperatorEn(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_debug", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_download_debug:
    FutuBatchOperatorCnDebug(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuBatchOperatorEnDebug(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_extract:
    FutuExtractHtml(
        from_key="company_profile_batch_download/",
        key=company_profile_batch_extract.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract_debug", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_extract_debug:
    FutuExtractHtmlDebug(
        from_key="company_profile_batch_download_debug/",
        key=company_profile_batch_extract_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
