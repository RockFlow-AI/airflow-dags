from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.symbol import *

DAG_ID = "symbol_download"

NASDAQ_RAW_KEY = f'{DAG_ID}_nasdaq'
HKEX_RAW_KEY = f'{DAG_ID}_hkex'
SSE_RAW_KEY = f'{DAG_ID}_sse'
SZSE_RAW_KEY = f'{DAG_ID}_szse'

SYMBOL_PARSE_KEY = f'{DAG_ID}_parse/'
MERGE_CSV_KEY = f'{DAG_ID}_merge/merge.csv'

with DAG(DAG_ID, default_args=DEFAULT_DEBUG_ARGS) as dag:
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

chain(
    [nasdaq, hkex, sse, szse],
    [nasdaq_parse, hkex_parse, sse_parse, szse_parse],
    merge_csv
)
