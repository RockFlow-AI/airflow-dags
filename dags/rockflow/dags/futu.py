from airflow.models import DAG
from airflow.models.baseoperator import chain

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

    extract_cn = FutuExtractHtml(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_cn = FutuFormatJsonCn(
        task_id="futu_formate_json_cn",
        from_key="{{ task_instance.xcom_pull('" + extract_cn.task_id + "') }}",
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

    extract_en = FutuExtractHtml(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_en = FutuFormatJsonEn(
        task_id="futu_formate_json_en",
        from_key="{{ task_instance.xcom_pull('" + extract_en.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

chain(
    [futu_cn, futu_en],
    [extract_cn, extract_en],
    [format_cn, format_en]
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

    extract_cn_debug = FutuExtractHtmlDebug(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_cn_debug = FutuFormatJsonCn(
        task_id="futu_formate_json_cn",
        from_key="{{ task_instance.xcom_pull('" + extract_cn_debug.task_id + "') }}",
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

    extract_en_debug = FutuExtractHtmlDebug(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_en_debug = FutuFormatJsonEn(
        task_id="futu_formate_json_en",
        from_key="{{ task_instance.xcom_pull('" + extract_en_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

chain(
    [futu_cn_debug, futu_en_debug],
    [extract_cn_debug, extract_en_debug],
    [format_cn_debug, format_en_debug]
)
