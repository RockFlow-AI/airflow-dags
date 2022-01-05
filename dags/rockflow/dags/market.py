from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.market import *

with DAG("market_download", catchup=False, default_args=DEFAULT_DEBUG_ARGS) as mic:
    mic_download = MicDownloadOperator(
        key=mic.dag_id
    )

    mic_to_mysql = MarketImportOperator(
        oss_source_key=mic_download.oss_key,
        mysql_table='flow_ticker_market',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    mic_download,
    mic_to_mysql,
)
