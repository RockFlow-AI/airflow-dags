from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.futu import *

with DAG("company_profile_batch_download_cn", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuBatchOperatorCn(
        from_key=MERGE_CSV_KEY,
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_en", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuBatchOperatorEn(
        from_key=MERGE_CSV_KEY,
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_cn_debug", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuBatchOperatorCnDebug(
        from_key=MERGE_CSV_KEY,
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_en_debug", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuBatchOperatorEnDebug(
        from_key=MERGE_CSV_KEY,
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuExtractHtml(
        from_key="company_profile_batch_download/",
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract_debug", default_args=DEFAULT_DEBUG_ARGS) as dag:
    FutuExtractHtmlDebug(
        from_key="company_profile_batch_download_debug/",
        key=dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
