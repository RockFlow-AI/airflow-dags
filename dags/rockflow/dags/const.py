import os
import sys
from datetime import datetime, timedelta

if 'unittest' in sys.modules:
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv(), override=True)
    print("os.environ:", {
        k: v for k, v in os.environ.items() if k.startswith("AIRFLOW")
    })

MYSQL_CONNECTION_FLOW_TICKER = 'mysql_flow_ticker'

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
