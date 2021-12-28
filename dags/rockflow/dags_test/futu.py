from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *

# 废弃
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
        from_key="{{ task_instance.xcom_pull('" + extract_en.task_id + "') }}",
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    join_map = JoinMap(
        first="{{ task_instance.xcom_pull('" + format_cn.task_id + "') }}",
        second="{{ task_instance.xcom_pull('" + format_en.task_id + "') }}",
        merge_key=MERGE_CSV_KEY,
        key=company_profile_batch_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + join_map.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_stock_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default',
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sink_futu_profile_op = SinkFutuProfile(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        oss_source_key="{{ task_instance.xcom_pull('" +
                       join_map.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    [futu_cn, futu_en],
    [extract_cn, extract_en],
    [format_cn, format_en],
    join_map,
    [sink_es, sink_futu_profile_op],
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

    extract_cn_debug = FutuExtractHtml(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" +
                 futu_cn_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_cn_debug = FutuFormatJsonCn(
        from_key="{{ task_instance.xcom_pull('" +
                 extract_cn_debug.task_id + "') }}",
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

    extract_en_debug = FutuExtractHtml(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" +
                 futu_en_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_en_debug = FutuFormatJsonEn(
        from_key="{{ task_instance.xcom_pull('" +
                 extract_en_debug.task_id + "') }}",
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    join_map_debug = JoinMap(
        first="{{ task_instance.xcom_pull('" +
              format_cn_debug.task_id + "') }}",
        second="{{ task_instance.xcom_pull('" +
               format_en_debug.task_id + "') }}",
        merge_key=MERGE_CSV_KEY,
        key=company_profile_batch_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sink_es_debug = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" +
                 join_map_debug.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_stock_search_debug',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default',
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sink_futu_profile_op_debug = SinkFutuProfile(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        oss_source_key="{{ task_instance.xcom_pull('" +
                       join_map_debug.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile_debug',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    [futu_cn_debug, futu_en_debug],
    [extract_cn_debug, extract_en_debug],
    [format_cn_debug, format_en_debug],
    join_map_debug,
    [sink_es_debug, sink_futu_profile_op_debug],
)