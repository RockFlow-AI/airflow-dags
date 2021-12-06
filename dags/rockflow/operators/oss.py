from typing import Optional, Any

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook


class OSSOperator(BaseOperator):
    def __init__(
            self,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy
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

    def execute(self, context: Any):
        raise NotImplementedError()
