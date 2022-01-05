import os
import sys

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
    "retries": 0,
}
