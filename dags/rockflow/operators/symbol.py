from typing import Optional

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

    def execute(self, context):
        nasdaq = Nasdaq(proxy=self.proxy)
        r = nasdaq._get()
        oss_hook = OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)
        oss_hook.upload_local_file(bucket_name=self.bucket_name, key=self.key, file=r.content)


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

    def execute(self, context):
        hkex = HKEX(proxy=self.proxy)
        r = hkex._get()
        oss_hook = OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)
        oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=r.content)
