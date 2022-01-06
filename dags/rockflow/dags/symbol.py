from datetime import datetime, timedelta

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

with DAG(
        DAG_ID,
        catchup=False,
        start_date=datetime(2022, 1, 1),
        schedule_interval=timedelta(hours=8),
        default_args={
            "owner": "daijunkai",
            "depends_on_past": False,
            "retries": 12,
            "retry_delay": timedelta(minutes=1),
        }
) as symbol_dag:
    # ------------------------------------------------------------

    nasdaq = NasdaqSymbolDownloadOperator(
        key=NASDAQ_RAW_KEY
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + nasdaq.task_id + "') }}",
        key=SYMBOL_PARSE_KEY
    )

    hkex = HkexSymbolDownloadOperator(
        key=HKEX_RAW_KEY
    )

    hkex_parse = HkexSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + hkex.task_id + "') }}",
        key=SYMBOL_PARSE_KEY
    )

    sse = SseSymbolDownloadOperator(
        key=SSE_RAW_KEY
    )

    sse_parse = SseSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + sse.task_id + "') }}",
        key=SYMBOL_PARSE_KEY
    )

    szse = SzseSymbolDownloadOperator(
        key=SZSE_RAW_KEY
    )

    szse_parse = SzseSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + szse.task_id + "') }}",
        key=SYMBOL_PARSE_KEY
    )

    merge_csv = MergeCsvList(
        from_key=SYMBOL_PARSE_KEY,
        key=MERGE_CSV_KEY
    )

    # ------------------------------------------------------------

    futu_cn = FutuBatchOperatorCn(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id
    )

    extract_cn = FutuExtractHtml(
        task_id="futu_extract_html_cn",
        from_key="{{ task_instance.xcom_pull('" + futu_cn.task_id + "') }}",
        key=symbol_dag.dag_id,
        pool_size=32
    )

    format_cn = FutuFormatJsonCn(
        from_key="{{ task_instance.xcom_pull('" + extract_cn.task_id + "') }}",
        key=symbol_dag.dag_id
    )

    futu_en = FutuBatchOperatorEn(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id
    )

    extract_en = FutuExtractHtml(
        task_id="futu_extract_html_en",
        from_key="{{ task_instance.xcom_pull('" + futu_en.task_id + "') }}",
        key=symbol_dag.dag_id,
        pool_size=32
    )

    format_en = FutuFormatJsonEn(
        from_key="{{ task_instance.xcom_pull('" + extract_en.task_id + "') }}",
        key=symbol_dag.dag_id
    )

    join_map = JoinMap(
        first="{{ task_instance.xcom_pull('" + format_cn.task_id + "') }}",
        second="{{ task_instance.xcom_pull('" + format_en.task_id + "') }}",
        merge_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + join_map.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_stock_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default'
    )

    sink_futu_profile_op = SinkFutuProfile(
        oss_source_key="{{ task_instance.xcom_pull('" +
                       join_map.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    # ------------------------------------------------------------

    yahoo = YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=symbol_dag.dag_id
    )

    sharding = 10
    for i in range(sharding):
        yahoo_extract = YahooExtractOperator(
            task_id=f"yahoo_extract_{i}",
            from_key="symbol_download_yahoo",
            key=symbol_dag.dag_id,
            symbol_key=MERGE_CSV_KEY,
            partition=i,
            sharding=sharding
        )

        summary_detail_mysql = SummaryDetailImportOperator(
            task_id=f"summary_detail_mysql_{i}",
            oss_source_key=yahoo_extract.save_key("SummaryDetail"),
            mysql_table='flow_ticker_summary_detail',
            mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
        )

        yahoo_extract.set_upstream(merge_csv)
        yahoo_extract.set_upstream(yahoo)
        yahoo_extract.set_downstream(summary_detail_mysql)

chain(
    [nasdaq, hkex, sse, szse],
    [nasdaq_parse, hkex_parse, sse_parse, szse_parse],
    merge_csv,
)

chain(
    merge_csv,
    [futu_cn, futu_en],
    [extract_cn, extract_en],
    [format_cn, format_en],
    join_map,
    [sink_es, sink_futu_profile_op],
)

chain(
    merge_csv,
    yahoo,
)
