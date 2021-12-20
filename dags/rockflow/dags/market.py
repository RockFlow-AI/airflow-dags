from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.market import *
from rockflow.operators.mysql import OssToMysqlOperator

with DAG("market_download", default_args=DEFAULT_DEBUG_ARGS) as mic:
    mic_download = MicDownloadOperator(
        key=mic.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

    mic_to_mysql = OssToMysqlOperator(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        oss_source_key=mic_download.oss_key,
        mapping={
            "MIC": "mic",
            "OPERATING MIC": "operating_mic",
            "NAME-INSTITUTION DESCRIPTION": "name",
            "ACRONYM": "acronym",
            "CITY": "city",
            "COUNTRY": "country",
            "WEBSITE": "website",
        },
        mysql_table='flow_ticker_market',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    mic_download,
    mic_to_mysql,
)
