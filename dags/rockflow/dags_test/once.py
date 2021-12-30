from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.rename import RenameOss

with DAG("rename", default_args=DEFAULT_DEBUG_ARGS) as rename:
    symbol_download_futu_company_profile_cn = RenameOss(
        task_id="symbol_download_futu_company_profile_cn",
        prefix="symbol_download_futu_company_profile_cn",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    symbol_download_futu_company_profile_en = RenameOss(
        task_id="symbol_download_futu_company_profile_en",
        prefix="symbol_download_futu_company_profile_en",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    symbol_download_yahoo = RenameOss(
        task_id="symbol_download_yahoo",
        prefix="symbol_download_yahoo",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
