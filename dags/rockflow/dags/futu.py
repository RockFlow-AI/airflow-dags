from airflow.models import DAG

from rockflow.dags.const import MERGE_CSV_KEY, DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS
from rockflow.operators.futu import *

with DAG("company_profile_cn_download", default_args=DEFAULT_DEBUG_ARGS) as cn_dag:
    futu_html_cn_prefix = "company_profile_cn_download"
    futu_cn = FutuCnOperator(
        key=futu_html_cn_prefix,
        ticker="00700-HK",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_en_download", default_args=DEFAULT_DEBUG_ARGS) as en_dag:
    futu_html_en_prefix = "company_profile_en_download"
    futu_en = FutuEnOperator(
        key=futu_html_en_prefix,
        ticker="00700-HK",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download", default_args=DEFAULT_DEBUG_ARGS) as batch_dag:
    futu_html_batch_prefix = "company_profile_batch_download"
    futu_batch = FutuBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=futu_html_batch_prefix,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract", default_args=DEFAULT_DEBUG_ARGS) as extract_dag:
    futu_html_extract_prefix = "company_profile_extract_download"
    futu_extract = FutuExtractHtml(
        from_key="company_profile_batch_download/",
        key=futu_html_extract_prefix,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_extract_debug", default_args=DEFAULT_DEBUG_ARGS) as extract_dag_debug:
    futu_html_extract_prefix_debug = "company_profile_batch_extract_debug"
    futu_extract_debug = FutuExtractHtmlDebug(
        from_key="company_profile_batch_download/",
        key=futu_html_extract_prefix_debug,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
