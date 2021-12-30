from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.logo import *

with DAG("logo_download", default_args=DEFAULT_DEBUG_ARGS) as logo_download:
    public_logo_download = PublicLogoBatchOperator(
        task_id="public_logo_download",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    public_logo_download_debug = PublicLogoBatchOperatorDebug(
        task_id="public_logo_download_debug",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    etoro_logo_download = EtoroLogoBatchOperator(
        task_id="etoro_logo_download",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    etoro_logo_download_debug = EtoroLogoBatchOperatorDebug(
        task_id="etoro_logo_download_debug",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id,
        region=logo_download,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
