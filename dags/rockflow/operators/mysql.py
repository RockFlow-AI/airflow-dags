from typing import Optional, Any, Dict

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pangres import upsert

from rockflow.common.pandas_helper import map_frame
from rockflow.operators.oss import OSSOperator


class OssToMysqlOperator(OSSOperator):
    def __init__(
            self,
            oss_source_key: str,
            mapping: Dict[str, str],
            mysql_table: str,
            mysql_conn_id: str = 'mysql_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oss_source_key = oss_source_key
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mapping = mapping

        self.mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    def execute_sql(self, cmd):
        conn = self.mysql_hook.get_conn()
        cur = conn.cursor()
        self.log.info(f"{cur.execute(cmd)}")
        conn.commit()

    def extract_data(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.oss_source_key))

    def get_sql_schema(self):
        self.execute_sql(f"SHOW CREATE TABLE {self.mysql_table}")

    def transform(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        self.log.info(f"{df[:10]}")
        result = map_frame(df, self.mapping)
        self.log.info(f"{result[:10]}")
        return result

    def load_to_sql(self, df: Optional[pd.DataFrame]):
        engine = self.mysql_hook.get_sqlalchemy_engine()
        return upsert(
            engine=engine,
            df=df,
            table_name=self.mysql_table,
            if_row_exists='update',
        )

    def execute(self, context: Any) -> None:
        self.log.info(
            f"Loading {self.oss_source_key} to MySql table {self.mysql_table}...")
        self.get_sql_schema()
        self.load_to_sql(
            self.transform(
                self.extract_data()
            )
        )
