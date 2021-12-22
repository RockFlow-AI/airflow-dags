from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.mysql import OssBatchToMysqlOperator, OssBatchToMysqlOperatorDebug

with DAG("history_sync", default_args=DEFAULT_DEBUG_ARGS) as history_sync:
    hkex_sync = OssBatchToMysqlOperator(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        prefix="day/hkex/",
        mapping={
            "Date": "begin",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    us_sync = OssBatchToMysqlOperator(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        prefix="day/us/",
        mapping={
            "Date": "begin",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

with DAG("history_sync_debug", default_args=DEFAULT_DEBUG_ARGS) as history_sync_debug:
    hkex_sync_debug = OssBatchToMysqlOperatorDebug(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        prefix="day/hkex/",
        mapping={
            "Date": "begin",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    us_sync_debug = OssBatchToMysqlOperatorDebug(
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        prefix="day/us/",
        mapping={
            "Date": "begin",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )
