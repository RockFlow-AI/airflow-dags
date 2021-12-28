from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.dags.symbol import MERGE_CSV_KEY
from rockflow.operators.yahoo import *

# 废弃
with DAG("yahoo_download", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download:
    yahoo = YahooBatchOperator(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    yahoo_extract = YahooExtractOperator(
        from_key="yahoo_download_yahoo",
        key="yahoo_extract",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

with DAG("yahoo_download_debug", default_args=DEFAULT_DEBUG_ARGS) as yahoo_download_debug:
    yahoo_debug = YahooBatchOperatorDebug(
        from_key=MERGE_CSV_KEY,
        key=yahoo_download_debug.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    yahoo_extract_debug = YahooExtractOperator(
        from_key="yahoo_download_yahoo",
        key="yahoo_extract_debug",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    summary_detail_mysql = SummaryDetailImportOperator(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        oss_source_key="yahoo_extract_summary_detail/summary_detail.json",
        mysql_table='flow_ticker_summary_detail',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    yahoo,
    yahoo_extract,
    summary_detail_mysql,
)

chain(
    yahoo_debug,
    yahoo_extract_debug,
)
