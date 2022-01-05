from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.operators.ice import DailyHistoryImportOperator
from rockflow.operators.sftp import SftpToOssOperator

ice_dag_args = {
    "owner": "daijunkai",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1, 23, 30),  # 北京时间6:30(bejing)开始跑
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
}

with DAG("ice_sftp_sync", catchup=True, default_args=ice_dag_args) as ice_sftp_sync:
    sync_all_files = SftpToOssOperator(
        prefix=ice_sftp_sync.dag_id,
        work_dir="/FLOWAIHKFTPH1",
        ssh_conn_id="ftp_ice"
    )

    daily_history = DailyHistoryImportOperator(
        oss_source_key=ice_sftp_sync.dag_id +
                       "/K16D75_{{ ds_nodash }}.SRV",  # 当天收盘跑当天的
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

chain(
    sync_all_files,
    daily_history,
)
