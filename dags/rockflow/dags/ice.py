from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.ice import DailyHistoryImportOperator
from rockflow.operators.sftp import SftpToOssOperator

with DAG("ice_sftp_sync", default_args=DEFAULT_DEBUG_ARGS) as ice_sftp_sync:
    SftpToOssOperator(
        prefix=ice_sftp_sync.dag_id,
        work_dir="/FLOWAIHKFTPH1",
        ssh_conn_id="ftp_ice"
    )

    daily_history = DailyHistoryImportOperator(
        task_id='summary_detail_mysql_us',
        oss_source_key="ice_sftp_sync/K16D75_20211227.SRV",
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )
