import json
import os
from typing import Optional, Any, Dict

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pangres import upsert

from rockflow.common.pandas_helper import map_frame
from rockflow.operators.oss import OSSOperator


class OssToMysqlOperator(OSSOperator):
    template_fields = ["oss_source_key"]

    def __init__(
            self,
            oss_source_key: str,
            index_col: Any,
            mapping: Dict[str, str],
            mysql_table: str,
            mysql_conn_id: str = 'mysql_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oss_source_key = oss_source_key
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.index_col = index_col
        self.mapping = mapping

        self.mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    def execute_sql(self, cmd):
        conn = self.mysql_hook.get_conn()
        cur = conn.cursor()
        self.log.info(f"{cur.execute(cmd)}")
        conn.commit()

    def extract_csv_to_df(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.oss_source_key))

    def extract_index_dict(self):
        return json.loads(
            self.get_object(self.oss_source_key).read()
        )

    def extract_index_dict_from_local_file(self):
        with open(self.oss_source_key) as f:
            return json.load(f)

    def extract_index_dict_to_df(self, dict_data) -> pd.DataFrame:
        result = pd.DataFrame.from_dict(
            dict_data,
            orient='index'
        )
        result.index.rename(self.index_col, inplace=True)
        self.log.error(result[result.index.duplicated(keep=False)])
        return result

    def extract_data(self) -> pd.DataFrame:
        return self.extract_csv_to_df()

    def get_sql_schema(self):
        self.execute_sql(f"SHOW CREATE TABLE {self.mysql_table}")

    def transform(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        self.log.info(f"{df}")
        result = map_frame(df, self.mapping)
        self.log.info(f"{result}")
        result.set_index(self.index_col, inplace=True)
        return result

    def load_to_sql(self, df: Optional[pd.DataFrame]):
        engine = self.mysql_hook.get_sqlalchemy_engine()
        return upsert(
            engine,
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


class OssBatchToMysqlOperator(OSSOperator):
    def __init__(
            self,
            prefix: str,
            mapping: Dict[str, str],
            mysql_table: str,
            mysql_conn_id: str = 'mysql_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mapping = mapping
        self.mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    def extract_data(self, obj) -> pd.DataFrame:
        return pd.read_csv(self.get_object(obj.key))

    def transform(self, obj, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        self.log.info(f"{df}")
        result = map_frame(df, self.mapping)
        self.log.info(f"{result}")
        return result

    def load_to_sql(self, obj, df: Optional[pd.DataFrame]):
        engine = self.mysql_hook.get_sqlalchemy_engine()
        return upsert(
            engine,
            df=df,
            table_name=self.mysql_table,
            if_row_exists='update',
        )

    def iterator(self):
        return self.path_object_iterator(self.prefix)

    def execute(self, context: Any) -> None:
        for obj in self.iterator():
            if obj.key.endswith("/"):
                continue
            self.load_to_sql(
                obj,
                self.transform(
                    obj,
                    self.extract_data(obj)
                )
            )


class OssBatchToMysqlOperatorDebug(OssBatchToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def iterator(self):
        from itertools import islice
        return islice(super().iterator(), 10)


class MysqlToOssOperator(OSSOperator):
    template_fields = ["oss_src_key"]

    def __init__(
            self,
            oss_src_key: str,
            oss_dst_key: str,
            key: str,
            index_col: Any,
            mysql_table: str,
            mysql_conn_id: str = 'mysql_default',
            mysql_criteria: str = '',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oss_src_key = oss_src_key
        self.oss_dst_key = os.path.join(
            f"{key}_{oss_dst_key}",
            f"{oss_dst_key}.json"
        )
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_criteria = mysql_criteria
        self.index_col = index_col

        self.mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    def __extract_index_dict(self):
        return json.loads(
            self.get_object(self.oss_src_key).read()
        )

    def __extract_index_dict_to_df(self, dict_data) -> pd.DataFrame:
        result = pd.DataFrame.from_dict(
            dict_data,
            orient='index'
        )
        result.index.rename(self.index_col, inplace=True)
        self.log.error(result[result.index.duplicated(keep=False)])
        return result

    def extract_data(self) -> pd.DataFrame:
        return self.__extract_index_dict_to_df(
            self.__extract_index_dict()
        )

    def __load_from_sql(self):
        conn = self.mysql_hook.get_conn()
        cur = conn.cursor()

        df = pd.DataFrame(columns=['symbol', 'raw', 'name_en', 'name_zh'])
        cur.execute(f"SELECT symbol, raw, name_en, name_zh FROM {self.mysql_table} {self.mysql_criteria}")

        result = cur.fetchmany(100)
        while result:
            self.log.info(f"Fetched from {self.mysql_table}: {result}")
            df = df.append(pd.DataFrame(result, columns=['symbol', 'raw', 'name_en', 'name_zh']), ignore_index=True)
            result = cur.fetchmany(100)
        return df

    def __transform(self):
        df_oss = self.extract_data()
        df_db = self.__load_from_sql()
        if df_oss.empty:
            df_db['name'] = df_db['symbol']
            df_db.set_index('name', inplace=True)
            return df_db.to_json(orient='index', force_ascii=False)

        df_oss['name_en'] = df_oss['symbol'].map(df_db.set_index('symbol')['name_en'])
        df_oss['name_cn'] = df_oss['symbol'].map(df_db.set_index('symbol')['name_zh'])
        return df_oss.to_json(orient='index', force_ascii=False)

    def execute(self, context: Any) -> str:
        self.log.info(f"Loading MySql table {self.mysql_table} to {self.oss_dst_key}...")
        self.put_object(key=self.oss_dst_key, content=self.__transform())
        return self.oss_dst_key
