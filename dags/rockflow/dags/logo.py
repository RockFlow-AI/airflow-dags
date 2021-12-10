from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.logo import *

with DAG("public_logo_download", default_args=DEFAULT_DEBUG_ARGS) as public:
    PublicLogoBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=public.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("public_logo_download_debug", default_args=DEFAULT_DEBUG_ARGS) as public_debug:
    PublicLogoBatchOperatorDebug(
        from_key=MERGE_CSV_KEY,
        key=public_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("etoro_logo_download", default_args=DEFAULT_DEBUG_ARGS) as etoro:
    EtoroLogoBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=etoro.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("etoro_logo_download_debug", default_args=DEFAULT_DEBUG_ARGS) as etoro_debug:
    EtoroLogoBatchOperatorDebug(
        from_key=MERGE_CSV_KEY,
        key=etoro_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
