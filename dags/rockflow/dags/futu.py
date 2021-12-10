from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.futu import *

with DAG("company_profile_batch_download", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_download:
    futu_cn = FutuBatchOperatorCn(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuExtractHtml(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    futu_en = FutuBatchOperatorEn(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuExtractHtml(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("company_profile_batch_download_debug",
         default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_download_debug:
    futu_cn_debug = FutuBatchOperatorCnDebug(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuExtractHtmlDebug(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    futu_en_debug = FutuBatchOperatorEnDebug(
        from_key=MERGE_CSV_KEY,
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    FutuExtractHtmlDebug(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
