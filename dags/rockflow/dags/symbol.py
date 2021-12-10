from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import MERGE_CSV_KEY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_PROXY, DEFAULT_DEBUG_ARGS
from rockflow.operators.symbol import *

with DAG("symbol_download", default_args=DEFAULT_DEBUG_ARGS) as dag:
    nasdaq_raw_key = 'airflow-symbol-raw-nasdaq/nasdaq.json'
    nasdaq_csv_key = 'airflow-symbol-csv-nasdaq/nasdaq.csv'
    symbol_parse_key = 'airflow-symbol-parse/'
    hkex_raw_key = 'airflow-symbol-raw-hkex/hkex.xlsx'
    hkex_csv_key = 'airflow-symbol-csv-hkex/hkex.csv'
    sse_raw_key = 'airflow-symbol-raw-sse/sse.xlsx'
    sse_csv_key = 'airflow-symbol-csv-sse/sse.csv'
    szse_raw_key = 'airflow-symbol-raw-szse/szse.xlsx'
    szse_csv_key = 'airflow-symbol-csv-szse/szse.csv'

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

    nasdaq_csv = NasdaqSymbolToCsv(
        from_key=nasdaq_raw_key,
        key=nasdaq_csv_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex_csv = HkexSymbolToCsv(
        from_key=hkex_raw_key,
        key=hkex_csv_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse_csv = SseSymbolToCsv(
        from_key=sse_raw_key,
        key=sse_csv_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse_csv = SzseSymbolToCsv(
        from_key=szse_raw_key,
        key=szse_csv_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key=nasdaq_csv_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    hkex_parse = HkexSymbolParser(
        from_key=hkex_csv_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    sse_parse = SseSymbolParser(
        from_key=sse_csv_key,
        key=symbol_parse_key,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    szse_parse = SzseSymbolParser(
        from_key=szse_csv_key,
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
    [nasdaq_csv, hkex_csv, sse_csv, szse_csv],
    [nasdaq_parse, hkex_parse, sse_parse, szse_parse],
    merge_csv
)
