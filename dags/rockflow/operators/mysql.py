from typing import Dict

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from rockflow.operators.oss import OSSOperator


class OssToMysqlOperator(OSSOperator):
    def __init__(
            self,
            oss_source_key: str,
            mysql_table: str,
            mysql_conn_id: str = 'mysql_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oss_source_key = oss_source_key
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id

        self.mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    @property
    def conn(self):
        return self.mysql_hook.get_conn()

    def execute(self, context: Dict) -> None:
        self.log.info(f"Loading {self.oss_source_key} to MySql table {self.mysql_table}...")

        data_df = pd.read_csv(
            self.get_object(self.oss_source_key)
        )

        sql_schema = pd.io.sql.get_schema(
            data_df,
            self.mysql_table,
            keys='MIC',
            dtype={},
            con=self.conn
        )

        self.log.info(f"{sql_schema}")

        data_df.to_sql(self.mysql_table, self.conn)
