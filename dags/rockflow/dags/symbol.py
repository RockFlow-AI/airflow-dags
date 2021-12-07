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

with DAG("symbol_download", default_args=default_args) as dag:
    Nasdaq = NasdaqSymbolDownloadOperator(
        task_id="download_nasdaq_symbol_to_local",
        key='airflow-symbol-raw-nasdaq/nasdaq.json',
        region=Variable.get("REGION"),
        bucket_name=Variable.get("BUCKET_NAME"),
        proxy=default_proxy()
    )

    Hkex = HkexSymbolDownloadOperator(
        task_id="download_hkex_symbol_to_local",
        key='airflow-symbol-raw-hkex/hkex.xlsx',
        region=Variable.get("REGION"),
        bucket_name=Variable.get("BUCKET_NAME"),
        proxy=default_proxy()
    )

    Nasdaq_csv = NasdaqSymbolToCSV(
        task_id="download_nasdaq_symbol_to_csv",
        from_key='airflow-symbol-raw-nasdaq/nasdaq.json',
        to_key='airflow-symbol-csv-nasdaq/nasdaq.csv',
        region=Variable.get("REGION"),
        bucket_name=Variable.get("BUCKET_NAME"),
        proxy=default_proxy()
    )

    Hkex_csv = HkexSymbolToCSV(
        task_id="download_hkex_symbol_to_csv",
        from_key='airflow-symbol-raw-hkex/hkex.xlsx',
        to_key='airflow-symbol-csv-hkex/hkex.csv',
        region=Variable.get("REGION"),
        bucket_name=Variable.get("BUCKET_NAME"),
        proxy=default_proxy()
    )

    merge_csv = MergeCsvList(
        task_id="merge_csvs",
        from_key_list=['airflow-symbol-csv-nasdaq/nasdaq.csv', 'airflow-symbol-csv-hkex/hkex.csv'],
        to_key='airflow-symbol-csv-merge/Merged.csv',
        region=Variable.get("REGION"),
        bucket_name=Variable.get("BUCKET_NAME"),
        proxy=default_proxy()
    )

# Nasdaq_csv.set_upstream(Nasdaq)
Hkex_csv.set_upstream(Hkex)
# merge_csv.set_upstream([Nasdaq_csv,Hkex_csv])
merge_csv.set_upstream(Hkex_csv)
