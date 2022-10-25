from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.baseoperator import chain

from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.symbol import *

from rockflow.operators.mysql import SqlAwareSinkCompany

DAG_ID = "symbol_download_cfd"

with DAG(
        DAG_ID,
        catchup=False,
        start_date=datetime.now(),
        schedule_interval="@once",
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=1),
        }
) as cfd_dag:
    cfd_sink_company = SqlAwareSinkCompany(
        oss_src_key=None,
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
        mysql_criteria='WHERE instrument = 7',
        key=cfd_dag.dag_id,
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + cfd_sink_company.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_quote_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default'
    )

chain(
    [cfd_sink_company],
    [sink_es],
)
