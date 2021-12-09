from datetime import datetime, timedelta
from typing import Optional

from airflow.models import DAG, Variable

from rockflow.common.proxy import Proxy
from rockflow.operators.futu import FutuCnOperator, FutuEnOperator, FutuBatchOperator

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
    "schedule_interval": "@once",  # for debug
    # "schedule_interval": "0 */12 * * *",
}


def default_proxy() -> Optional[dict]:
    return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies


region = Variable.get("REGION")
bucket_name = Variable.get("BUCKET_NAME")
proxy = default_proxy()

with DAG("company_profile_cn_download", default_args=default_args) as cn_dag:
    futu_html_cn_prefix = "company_profile_cn_download"
    futu_cn = FutuCnOperator(
        key=futu_html_cn_prefix,
        ticker="00700-HK",
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

with DAG("company_profile_en_download", default_args=default_args) as en_dag:
    futu_html_en_prefix = "company_profile_en_download"
    futu_en = FutuEnOperator(
        key=futu_html_en_prefix,
        ticker="00700-HK",
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )

with DAG("company_profile_batch_download", default_args=default_args) as batch_dag:
    futu_html_batch_prefix = "company_profile_batch_download"
    futu_batch = FutuBatchOperator(
        from_key='airflow-symbol-csv-merge/merge.csv',
        key=futu_html_batch_prefix,
        region=region,
        bucket_name=bucket_name,
        proxy=proxy
    )
