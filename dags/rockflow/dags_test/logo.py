from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.logo import *

with DAG("logo_download", default_args=DEFAULT_DEBUG_ARGS) as logo_download:
    public_logo_download = PublicLogoBatchOperator(
        task_id="public_logo_download",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id
    )

    public_logo_download_debug = PublicLogoBatchOperatorDebug(
        task_id="public_logo_download_debug",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id
    )

    etoro_logo_download = EtoroLogoBatchOperator(
        task_id="etoro_logo_download",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id
    )

    etoro_logo_download_debug = EtoroLogoBatchOperatorDebug(
        task_id="etoro_logo_download_debug",
        from_key=MERGE_CSV_KEY,
        key=logo_download.dag_id
    )
