from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.ftp import SftptToOssOperator

with DAG("ice_sftp_sync", default_args=DEFAULT_DEBUG_ARGS) as ice_sftp_sync:
    SftptToOssOperator(
        prefix=ice_sftp_sync.dag_id,
        work_dir="/FLOWAIHKFTPH1",
        ssh_conn_id="ftp_ice",
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
