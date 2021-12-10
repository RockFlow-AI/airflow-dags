from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.symbol import *

with DAG("symbol_download", default_args=DEFAULT_DEBUG_ARGS) as dag:
    nasdaq_raw_key = 'airflow-symbol-raw-nasdaq/nasdaq.json'
    symbol_parse_key = 'airflow-symbol-parse/'
    hkex_raw_key = 'airflow-symbol-raw-hkex/hkex.xlsx'
    sse_raw_key = 'airflow-symbol-raw-sse/sse.xlsx'
    szse_raw_key = 'airflow-symbol-raw-szse/szse.xlsx'

    nasdaq = NasdaqSymbolDownloadOperator(
        key=nasdaq_raw_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex = HkexSymbolDownloadOperator(
        key=hkex_raw_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse = SseSymbolDownloadOperator(
        key=sse_raw_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse = SzseSymbolDownloadOperator(
        key=szse_raw_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key=nasdaq_raw_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex_parse = HkexSymbolParser(
        from_key=hkex_raw_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse_parse = SseSymbolParser(
        from_key=sse_raw_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse_parse = SzseSymbolParser(
        from_key=szse_raw_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    merge_csv = MergeCsvList(
        from_key=symbol_parse_key,
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
