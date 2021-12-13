from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.market import *
from rockflow.operators.mysql import OssToMysqlOperator

with DAG("market_download", default_args=DEFAULT_DEBUG_ARGS) as mic:
    mic = MicDownloadOperator(
        key=mic.dag_id,
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )

#     mic_to_mysql = OssToMysqlOperator(
#         region=DEFAULT_REGION,
#         bucket_name=DEFAULT_BUCKET_NAME,
#         oss_source_key='',
#         mysql_table='flow_ticker_mic',
#         mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
#     )
#
# chain(
#     mic,
#     mic_to_mysql,
# )
