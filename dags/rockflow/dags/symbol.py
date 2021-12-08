from datetime import datetime, timedelta
from typing import Optional

from airflow.models import DAG, Variable
from airflow.models.baseoperator import chain

from rockflow.common.proxy import Proxy
from rockflow.operators.symbol import *

default_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    # "start_date": datetime(2021, 12, 8),
    "start_date": datetime.now(),
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    # "schedule_interval": "0 */12 * * *",
}


def default_proxy() -> Optional[dict]:
    return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies


region = Variable.get("REGION")
bucket_name = Variable.get("BUCKET_NAME")
proxy = default_proxy()

with DAG("symbol_download", default_args=default_args) as dag:
    nasdaq_raw_key = 'airflow-symbol-raw-nasdaq/nasdaq.json'
    nasdaq_csv_key = 'airflow-symbol-csv-nasdaq/nasdaq.csv'
    symbol_parse_key = 'airflow-symbol-parse'
    hkex_raw_key = 'airflow-symbol-raw-hkex/hkex.xlsx'
    hkex_csv_key = 'airflow-symbol-csv-hkex/hkex.csv'
    sse_raw_key = 'airflow-symbol-raw-sse/sse.xlsx'
    sse_csv_key = 'airflow-symbol-csv-sse/sse.csv'
    szse_raw_key = 'airflow-symbol-raw-szse/szse.xlsx'
    szse_csv_key = 'airflow-symbol-csv-szse/szse.csv'
    merge_csv_key = 'airflow-symbol-csv-merge/merge.csv'

    nasdaq = NasdaqSymbolDownloadOperator(
        key=nasdaq_raw_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    hkex = HkexSymbolDownloadOperator(
        key=hkex_raw_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    sse = SseSymbolDownloadOperator(
        key=sse_raw_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    szse = SzseSymbolDownloadOperator(
        key=szse_raw_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    nasdaq_csv = NasdaqSymbolToCsv(
        from_key=nasdaq_raw_key,
        key=nasdaq_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    hkex_csv = HkexSymbolToCsv(
        from_key=hkex_raw_key,
        key=hkex_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    sse_csv = SseSymbolToCsv(
        from_key=sse_raw_key,
        key=sse_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    szse_csv = SzseSymbolToCsv(
        from_key=szse_raw_key,
        key=szse_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key=nasdaq_csv_key,
        key=symbol_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    hkex_parse = HkexSymbolParser(
        from_key=hkex_csv_key,
        key=symbol_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    sse_parse = SseSymbolParser(
        from_key=sse_csv_key,
        key=symbol_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    szse_parse = SzseSymbolParser(
        from_key=szse_csv_key,
        key=symbol_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    merge_csv = MergeCsvList(
        from_key=symbol_parse_key,
        key=merge_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

chain(
    [nasdaq, hkex, sse, szse],
    [nasdaq_csv, hkex_csv, sse_csv, szse_csv],
    [nasdaq_parse, hkex_parse, sse_parse, szse_parse],
    merge_csv
)
