from typing import Dict, Optional

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

    @property
    def cursor(self):
        return self.conn.cursor()

    def read_data(self) -> pd.DataFrame:
        return pd.read_csv(
            self.get_object(self.oss_source_key)
        )

    @property
    def dtype(self):
        return None

    def execute_sql(self, cmd):
        self.log.info(f"{self.cursor.execute(cmd)}")

    def sql_schema(self):
        self.execute_sql(f"SHOW CREATE TABLE {self.mysql_table}")

    def to_sql(self, df: Optional[pd.DataFrame]):
        return df.to_sql(
            name=self.mysql_table,
            con=self.conn,
            if_exists="append",
        )

    def execute(self, context: Dict) -> None:
        self.log.info(f"Loading {self.oss_source_key} to MySql table {self.mysql_table}...")
        self.sql_schema()
        self.to_sql(self.read_data())
