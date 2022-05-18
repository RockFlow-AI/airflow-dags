from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.symbol import *
from rockflow.operators.yahoo import *
import pendulum

DAG_ID = "symbol_download_nasdaq"

NASDAQ_RAW_KEY = f'{DAG_ID}_nasdaq'

SYMBOL_PARSE_KEY = f'{DAG_ID}_parse/'

with DAG(
        DAG_ID,
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 18, tz='America/New_York'),
        schedule_interval='50 23 * * 1-5',
        default_args={
            "owner": "daijunkai",
            "depends_on_past": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=1),
        }
) as symbol_dag:
    # ------------------------------------------------------------

    symbol_raw = NasdaqSymbolDownloadOperator(
        key=NASDAQ_RAW_KEY
    )

    symbol_source = NasdaqSymbolParser(
        from_key="{{ task_instance.xcom_pull('" + symbol_raw.task_id + "') }}",
        key=SYMBOL_PARSE_KEY
    )

    # ------------------------------------------------------------

    futu_cn = FutuBatchOperatorCn(
        from_key="{{ task_instance.xcom_pull('" + symbol_source.task_id + "') }}",
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
        from_key="{{ task_instance.xcom_pull('" + symbol_source.task_id + "') }}",
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
        merge_key="{{ task_instance.xcom_pull('" + symbol_source.task_id + "') }}",
        key=symbol_dag.dag_id
    )

    sink_futu_rename_company = SinkFutuRenameCompany(
        oss_src_key="{{ task_instance.xcom_pull('" + join_map.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
        key=symbol_dag.dag_id,
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + sink_futu_rename_company.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_quote_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default'
    )

    sink_futu_profile_op = SinkFutuProfile(
        oss_source_key="{{ task_instance.xcom_pull('" + join_map.task_id + "') }}",
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    # ------------------------------------------------------------

    yahoo = yahoo_task_partition(
        shards=10,
        key=symbol_dag.dag_id,
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
        upstream=symbol_source,
    )

chain(
    symbol_raw,
    symbol_source,
)

chain(
    symbol_source,
    [futu_cn, futu_en],
    [extract_cn, extract_en],
    [format_cn, format_en],
    join_map,
    [sink_futu_profile_op],
    [sink_futu_rename_company],
    [sink_es],
)

chain(
    symbol_source,
    yahoo,
)
