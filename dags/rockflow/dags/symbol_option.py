from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.baseoperator import chain

import pandas as pd
from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.mysql import MysqlToOssOperator
from rockflow.operators.symbol import *

DAG_ID = "symbol_download_option"


class OptionSinkCompany(MysqlToOssOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'oss_dst_key' not in kwargs:
            kwargs['oss_dst_key'] = self.snakecase_class_name
        super().__init__(**kwargs)

    def extract_data(self) -> pd.DataFrame:
        self.log.info('Extracting data...')
        return pd.DataFrame()

    def post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log.info('Post processing...')
        for _, x in df.iterrows():
            try:
                # sample symbol: 'IBM   220708C00135500'
                option_symbol = x['symbol']
                strike = f"{option_symbol[13:18]}.{option_symbol[18:21]}".rstrip('.0')
                is_call = "C" == option_symbol[12:13]
                op_en = 'Over' if is_call else 'Below'
                op_zh = '涨过' if is_call else '跌过'
                maturity_date = f"{option_symbol[6:12]}"

                underlying = super().load_one_from_sql(option_symbol[:6].rstrip(' '))
                x['name_en'] = f"{underlying['symbol']} {x['name_en']} {op_en} {strike} {maturity_date}"
                x['name_zh'] = f"{underlying['symbol']} {x['name_zh']} {op_zh} {strike} {maturity_date}"
                self.log.info(f"Option name in en: {x['name_en']} and in zh: {x['name_zh']}")
            except ValueError:
                self.log.warn(f"No underlying found for option {x['symbol']}")
        return df


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
) as option_dag:
    option_sink_company = OptionSinkCompany(
        oss_src_key=None,
        mysql_table='flow_ticker_stock_profile',
        mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER,
        mysql_criteria='WHERE instrument = 2',
        key=option_dag.dag_id,
    )

    sink_es = SinkFutuSearch(
        from_key="{{ task_instance.xcom_pull('" + option_sink_company.task_id + "') }}",
        elasticsearch_index_name='i_flow_ticker_quote_search',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default'
    )

chain(
    [option_sink_company],
    [sink_es],
)
