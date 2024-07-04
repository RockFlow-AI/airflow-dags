from datetime import datetime

from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.const import AVATAR_BUCKET_NAME
from rockflow.operators.logo_gen import LogoImportOperator

with DAG(
        "black_logo_download",
        catchup=False,
        start_date=datetime.now(),
        schedule_interval="@once",
        default_args=DEFAULT_DEBUG_ARGS
) as black_logo_download:

    black_logo_import = LogoImportOperator(
        task_id="black_logo_import",
        from_key="company/black/symbols/stocks.txt",
        bucket_name=AVATAR_BUCKET_NAME,
        avatar_bucket_name=AVATAR_BUCKET_NAME,
        source_folder="company/black/source/",
        target_folder="company/black/public/",
    )
