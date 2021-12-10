from datetime import datetime, timedelta

from airflow.models import Variable

from rockflow.common.proxy import Proxy

MERGE_CSV_KEY = 'airflow-symbol-csv-merge/merge.csv'

DEFAULT_PROXY = Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies
DEFAULT_REGION = Variable.get("REGION")
DEFAULT_BUCKET_NAME = Variable.get("BUCKET_NAME")

DEFAULT_DEBUG_ARGS = {
    "owner": "daijunkai",
    "depends_on_past": False,
    # "start_date": datetime(2021, 12, 8),
    "start_date": datetime.now(),
    "email": ["daijunkai@flowcapai.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@once",  # for debug
    # "schedule_interval": "0 */12 * * *",
}
