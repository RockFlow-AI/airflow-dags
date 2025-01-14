from datetime import datetime
from datetime import timedelta

from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.const import AVATAR_BUCKET_NAME
from rockflow.operators.logo_gen import LogoImportOperator
from rockflow.operators.clear_file_content import ClearFileContentOperator

with DAG(
        "logo_download",
        catchup=False,
        start_date=datetime.now(),
        schedule_interval=timedelta(minutes=1),
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
        source_folder="company/source/",
        target_folder="company/public/",
    )

    black_logo_import = LogoImportOperator(
        task_id="black_logo_import",
        from_key="company/symbols/stocks.txt",
        bucket_name=AVATAR_BUCKET_NAME,
        avatar_bucket_name=AVATAR_BUCKET_NAME,
        source_folder="company/black/source/",
        target_folder="company/black/public/",
    )

    clear_logo_file_content = ClearFileContentOperator(
        task_id='clear_logo_file_content',
        from_key="company/symbols/stocks.txt",
        avatar_bucket_name=AVATAR_BUCKET_NAME,
    )

    logo_import >> clear_logo_file_content
    black_logo_import >> clear_logo_file_content

