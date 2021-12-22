from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.rename import RenameOss

with DAG("futu_rename_cn", default_args=DEFAULT_DEBUG_ARGS) as futu_rename_cn:
    RenameOss(
        prefix="company_profile_batch_download_futu_company_profile_cn",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("futu_rename_en", default_args=DEFAULT_DEBUG_ARGS) as futu_rename_en:
    RenameOss(
        prefix="company_profile_batch_download_futu_company_profile_en",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("yahoo_rename", default_args=DEFAULT_DEBUG_ARGS) as yahoo_rename:
    RenameOss(
        prefix="yahoo_download_yahoo",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
