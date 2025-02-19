from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.providers.http.operators.http import SimpleHttpOperator

import pandas as pd
from rockflow.dags.const import *
from rockflow.es_indexs.search import search_setting
from rockflow.operators.futu import *
from rockflow.operators.mysql import MysqlToOssOperator
from rockflow.operators.symbol import *
import pendulum

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
        for i, x in df.iterrows():
            try:
                # sample symbol: 'IBM   220708C00135500'
                option_symbol = x['symbol']
                strike = f"{option_symbol[13:18]}.{option_symbol[18:21]}".strip('0').strip('.')
                is_call = "C" == option_symbol[12:13]
                op_en = 'Over' if is_call else 'Below'
                op_zh = '涨过' if is_call else '跌过'
                maturity_date = f"{option_symbol[6:12]}"

                underlying = super().load_one_from_sql(option_symbol[:6].rstrip(' '))
                x['name_en'] = f"{underlying['symbol']} {underlying['name_en']} {op_en} {strike} {maturity_date}"
                x['name_zh'] = f"{underlying['symbol']} {underlying['name_zh']} {op_zh} {strike} {maturity_date}"
                df.at[i, 'expiry_date'] = datetime.strptime(maturity_date, '%y%m%d').strftime('%Y-%m-%d')
                self.log.info(f"Option name in en: {x['name_en']} and in zh: {x['name_zh']}")
            except ValueError:
                self.log.warn(f"No underlying found for option {x['symbol']}")
        return df


with DAG(
        DAG_ID,
        catchup=False,
        start_date=pendulum.datetime(2022, 7, 16, tz='America/New_York'),
        schedule_interval='50 23 * * 1-5',
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

option_chain_us = DAG(
    "option_chain_us",
    catchup=False,
    start_date=pendulum.datetime(2022, 2, 28, tz='America/New_York'),
    schedule_interval='0 20 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

option_chain_us_task = SimpleHttpOperator(
    task_id='option_chain',
    method='PATCH',
    http_conn_id='flow-feed-tick-ice',
    endpoint='/ice/inner/snapshots',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_chain_us,
)


option_chain_us_fiu = DAG(
    "option_chain_us_fiu",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 13, tz='America/New_York'),
    schedule_interval='0 15 * * 0',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

option_chain_us_fiu_task = SimpleHttpOperator(
    task_id='option_chain_us',
    method='PATCH',
    http_conn_id='flow-feed-tick-fiu',
    endpoint='/tick/inner/options/chains',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_chain_us_fiu,
)
