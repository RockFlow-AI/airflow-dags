from datetime import datetime, timedelta

from airflow.models import DAG

from rockflow.operators.symbol import NasdaqSymbolDownloadOperator, HkexSymbolDownloadOperator

default_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 6),
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
}

with DAG("symbol_download", default_args=default_args) as dag:
    Nasdaq = NasdaqSymbolDownloadOperator(
        task_id="download_nasdaq_symbol_to_local"
    )

    Hkex = HkexSymbolDownloadOperator(
        task_id="download_hkex_symbol_to_local"
    )

[Nasdaq, Hkex]
