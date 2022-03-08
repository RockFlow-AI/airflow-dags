from datetime import datetime, timedelta

from airflow.models import DAG

from rockflow.operators.sftp import SftpToOssOperator

prefix = "ice_sftp_sync"

with DAG(
        "ice_sftp_sync_day",
        catchup=True,
        start_date=datetime(2022, 1, 1, 22, 30),  # 北京时间6:30(bejing)开始跑
        schedule_interval=timedelta(days=1),
        default_args={
            "owner": "daijunkai",
            "depends_on_past": False,
            "retries": 0,
        }
) as ice_sftp_sync_day:
    sync_all_files = SftpToOssOperator(
        prefix=prefix,
        work_dir="/FLOWAIHKFTPH1",
        ssh_conn_id="ftp_ice"
    )

with DAG(
        "ice_sftp_sync_night",
        catchup=True,
        start_date=datetime(2022, 1, 1, 11, 30),  # 北京时间19:30(bejing)开始跑
        schedule_interval=timedelta(days=1),
        default_args={
            "owner": "daijunkai",
            "depends_on_past": False,
            "retries": 0,
        }
) as ice_sftp_sync_night:
    sync_all_files = SftpToOssOperator(
        prefix=prefix,
        work_dir="/FLOWAIHKFTPH1",
        ssh_conn_id="ftp_ice"
    )
