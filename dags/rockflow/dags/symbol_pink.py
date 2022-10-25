from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.baseoperator import chain

import pandas as pd
from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.mysql import MysqlToOssOperator
from rockflow.operators.symbol import *

DAG_ID = "symbol_download_pink"


class SqlAwareSinkCompany(MysqlToOssOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'oss_dst_key' not in kwargs:
            kwargs['oss_dst_key'] = self.snakecase_class_name
        super().__init__(**kwargs)

    def extract_data(self) -> pd.DataFrame:
        return pd.DataFrame()


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
) as pink_dag:
    pink_sink_company = SqlAwareSinkCompany(
        oss_src_key=None,
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
        mysql_criteria='WHERE instrument = 1 AND symbol = "DIDIY"',
        key=pink_dag.dag_id,
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + pink_sink_company.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_quote_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default'
    )

chain(
    [pink_sink_company],
    [sink_es],
)
