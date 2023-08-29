from datetime import datetime

from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.const import AVATAR_BUCKET_NAME
from rockflow.operators.logo_gen import LogoImportOperator

with DAG(
        "logo_download",
        catchup=False,
        start_date=datetime.now(),
        schedule_interval="@once",
        default_args=DEFAULT_DEBUG_ARGS
) as logo_download:
    # public_logo_download = PublicLogoBatchOperator(
    #     task_id="public_logo_download",
    #     from_key=MERGE_CSV_KEY,
    #     key=logo_download.dag_id
    # )
    #
    # public_logo_download_debug = PublicLogoBatchOperatorDebug(
    #     task_id="public_logo_download_debug",
    #     from_key=MERGE_CSV_KEY,
    #     key=logo_download.dag_id
    # )
    #
    # etoro_logo_download = EtoroLogoBatchOperator(
    #     task_id="etoro_logo_download",
    #     from_key=MERGE_CSV_KEY,
    #     key=logo_download.dag_id
    # )
    #
    # etoro_logo_download_debug = EtoroLogoBatchOperatorDebug(
    #     task_id="etoro_logo_download_debug",
    #     from_key=MERGE_CSV_KEY,
    #     key=logo_download.dag_id
    # )

    logo_import = LogoImportOperator(
        task_id="logo_import",
        from_key="company/symbols/stocks.txt",
        bucket_name=AVATAR_BUCKET_NAME,
        avatar_bucket_name=AVATAR_BUCKET_NAME,
    )
