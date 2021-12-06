from typing import Optional

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq


class NasdaqSymbolDownloadOperator(BaseOperator):
    def __init__(
            self,
            key: str,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy
        self.key = key
        self.oss_conn_id = oss_conn_id
        self.region = region
        self.bucket_name = bucket_name

    @property
    def oss_hook(self):
        return OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)

    def execute(self, context):
        r = Nasdaq(proxy=self.proxy).get()
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=r.content)


class NasdaqSymbolToCSV(BaseOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy
        self.from_key = from_key
        self.to_key = to_key
        self.oss_conn_id = oss_conn_id
        self.region = region
        self.bucket_name = bucket_name

    @property
    def oss_hook(self):
        return OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)

    def get_object(self, key):
        try:
            self.oss_hook.get_bucket(self.bucket_name).get_object(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def execute(self, context):
        raw_df = Nasdaq().to_df(self.get_object(self.from_key))
        self.oss_hook.load_string(self.to_key, raw_df.to_csv())


class HkexSymbolDownloadOperator(BaseOperator):
    def __init__(
            self,
            key: str,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy
        self.key = key
        self.oss_conn_id = oss_conn_id
        self.region = region
        self.bucket_name = bucket_name

    @property
    def oss_hook(self):
        return OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)

    def execute(self, context):
        r = HKEX(proxy=self.proxy).get()
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=r.content)


class HkexSymbolToCSV(BaseOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy
        self.from_key = from_key
        self.to_key = to_key
        self.oss_conn_id = oss_conn_id
        self.region = region
        self.bucket_name = bucket_name

    @property
    def oss_hook(self):
        return OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)

    def get_object(self, key):
        try:
            self.oss_hook.get_bucket(self.bucket_name).get_object(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def execute(self, context):
        raw_df = HKEX().to_df(self.get_object(self.from_key))
        self.oss_hook.load_string(self.to_key, raw_df.to_csv())
