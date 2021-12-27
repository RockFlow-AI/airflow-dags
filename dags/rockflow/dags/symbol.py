from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.symbol import *
from rockflow.operators.yahoo import *

DAG_ID = "symbol_download"

NASDAQ_RAW_KEY = f'{DAG_ID}_nasdaq'
HKEX_RAW_KEY = f'{DAG_ID}_hkex'
SSE_RAW_KEY = f'{DAG_ID}_sse'
SZSE_RAW_KEY = f'{DAG_ID}_szse'

SYMBOL_PARSE_KEY = f'{DAG_ID}_parse/'
MERGE_CSV_KEY = f'{DAG_ID}_merge/merge.csv'

symbol_dag_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 21),
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@hourly",
}

with DAG(DAG_ID, default_args=symbol_dag_args) as symbol_dag:
    # ------------------------------------------------------------

    nasdaq = NasdaqSymbolDownloadOperator(
        key=NASDAQ_RAW_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + nasdaq.task_id + "') }}",
        key=SYMBOL_PARSE_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex = HkexSymbolDownloadOperator(
        key=HKEX_RAW_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex_parse = HkexSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + hkex.task_id + "') }}",
        key=SYMBOL_PARSE_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse = SseSymbolDownloadOperator(
        key=SSE_RAW_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse_parse = SseSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + sse.task_id + "') }}",
        key=SYMBOL_PARSE_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse = SzseSymbolDownloadOperator(
        key=SZSE_RAW_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse_parse = SzseSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + szse.task_id + "') }}",
        key=SYMBOL_PARSE_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    merge_csv = MergeCsvList(
        from_key=SYMBOL_PARSE_KEY,
        key=MERGE_CSV_KEY,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    # ------------------------------------------------------------

    futu_cn = FutuBatchOperatorCn(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    extract_cn = FutuExtractHtml(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn.task_id + "') }}",
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_cn = FutuFormatJsonCn(
        from_key="{{ task_instance.xcom_pull('" + extract_cn.task_id + "') }}",
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    futu_en = FutuBatchOperatorEn(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    extract_en = FutuExtractHtml(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en.task_id + "') }}",
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    format_en = FutuFormatJsonEn(
        from_key="{{ task_instance.xcom_pull('" + extract_en.task_id + "') }}",
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    join_map = JoinMap(
        first="{{ task_instance.xcom_pull('" + format_cn.task_id + "') }}",
        second="{{ task_instance.xcom_pull('" + format_en.task_id + "') }}",
        merge_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id,
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
        oss_source_key="{{ task_instance.xcom_pull('" + join_map.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    # ------------------------------------------------------------

    yahoo = YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    yahoo_extract = YahooExtractOperator(
        from_key="yahoo_download_yahoo",
        key="yahoo_extract",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

chain(
    [nasdaq, hkex, sse, szse],
    [nasdaq_parse, hkex_parse, sse_parse, szse_parse],
    merge_csv,
    [futu_cn, futu_en, yahoo],
    [extract_cn, extract_en, yahoo_extract],
    [format_cn, format_en],
    join_map,
    [sink_es, sink_futu_profile_op],
)
