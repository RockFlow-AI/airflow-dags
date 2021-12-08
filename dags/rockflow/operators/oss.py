from typing import Optional, Any

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook
from stringcase import snakecase


class OSSOperator(BaseOperator):
    def __init__(
            self,
            region: str,
            bucket_name: Optional[str] = None,
            oss_conn_id: Optional[str] = 'oss_default',
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        if 'task_id' not in kwargs:
            kwargs['task_id'] = snakecase(self.__class__.__name__)
        super().__init__(**kwargs)
        self.proxy = proxy
        self.oss_conn_id = oss_conn_id
        self.region = region
        self.bucket_name = bucket_name

        self.oss_hook = OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)

        print(f"OSSOperator __dict__: {self.__dict__}")

    def get_object(self, key):
        try:
            return self.oss_hook.get_bucket(self.bucket_name).get_object(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def execute(self, context: Any):
        raise NotImplementedError()


class OSSSaveOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    @property
    def content(self):
        raise NotImplementedError()

    def execute(self, context):
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=self.content)
