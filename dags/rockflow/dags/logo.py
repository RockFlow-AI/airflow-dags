from airflow.models import DAG

from rockflow.dags.const import DEFAULT_PROXY, DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_DEBUG_ARGS, MERGE_CSV_KEY
from rockflow.operators.logo import PublicLogoBatchOperator, EtoroLogoBatchOperator

with DAG("public_logo_download", default_args=DEFAULT_DEBUG_ARGS) as public:
    PublicLogoBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=public.dag_id,
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
