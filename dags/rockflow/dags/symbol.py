from datetime import datetime, timedelta

from airflow.models import DAG, Variable

from rockflow.common.proxy import default_proxy
from rockflow.operators.symbol import *

default_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    "start_date": datetime.now(),  # TODO for debug 正式上线需要切换成正式时间
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    # "schedule_interval": "@daily",
}

region = Variable.get("REGION")
bucket_name = Variable.get("BUCKET_NAME")
proxy = default_proxy()

with DAG("symbol_download", default_args=default_args) as dag:
    nasdaq_raw_key = 'airflow-symbol-raw-nasdaq/nasdaq.json'
    nasdaq_csv_key = 'airflow-symbol-csv-nasdaq/nasdaq.csv'
    nasdaq_parse_key = 'airflow-symbol-parse-nasdaq/nasdaq.csv'
    hkex_raw_key = 'airflow-symbol-raw-hkex/hkex.xlsx'
    hkex_csv_key = 'airflow-symbol-csv-hkex/hkex.csv'
    hkex_parse_key = 'airflow-symbol-parse-hkex/hkex.csv'
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

    nasdaq_csv = NasdaqSymbolToCSV(
        from_key=nasdaq_raw_key,
        to_key=nasdaq_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    hkex_csv = HkexSymbolToCSV(
        from_key=hkex_raw_key,
        to_key=hkex_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    nasdaq_parse = NasdaqSymbolParser(
        from_key=nasdaq_csv_key,
        to_key=nasdaq_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    hkex_parse = HkexSymbolParser(
        from_key=hkex_csv_key,
        to_key=hkex_parse_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

    merge_csv = MergeCsvList(
        from_key_list=[nasdaq_parse_key, hkex_parse_key],
        to_key=merge_csv_key,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

nasdaq_csv.set_upstream(nasdaq)
hkex_csv.set_upstream(hkex)
nasdaq_parse.set_upstream(nasdaq_csv)
hkex_parse.set_upstream(hkex_csv)
merge_csv.set_upstream([nasdaq_parse, hkex_parse])
