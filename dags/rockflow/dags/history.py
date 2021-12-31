from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.operators.history import HistoryImportOperatorDebug, HistoryImportOperator

with DAG("history_sync", default_args=DEFAULT_DEBUG_ARGS) as history_sync:
    hkex_sync = HistoryImportOperator(
        task_id='sync_hkex_history_to_mysql',
        prefix="day/hkex/",
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    us_sync = HistoryImportOperator(
        task_id='sync_us_history_to_mysql',
        prefix="day/us/",
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

with DAG("history_sync_debug", default_args=DEFAULT_DEBUG_ARGS) as history_sync_debug:
    hkex_sync_debug = HistoryImportOperatorDebug(
        task_id='sync_hkex_history_to_mysql',
        prefix="day/hkex/",
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )

    us_sync_debug = HistoryImportOperatorDebug(
        task_id='sync_us_history_to_mysql',
        prefix="day/us/",
        mysql_table='flow_ticker_stock_price_daily',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
    )
