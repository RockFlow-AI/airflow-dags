from typing import Optional, Any

import oss2
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

    @property
    def snakecase_class_name(self):
        return snakecase(self.__class__.__name__)

    @property
    def lowercase_class_name(self):
        return self.__class__.__name__.lower()

    @property
    def bucket(self) -> oss2.api.Bucket:
        return self.oss_hook.get_bucket(self.bucket_name)

    @staticmethod
    def object_iterator_(bucket: oss2.api.Bucket, prefix: str):
        try:
            print(f"object_iterator: {prefix}")
            return oss2.ObjectIterator(bucket, prefix=prefix, delimiter='/')
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def object_iterator(self, prefix: str):
        return self.object_iterator_(self.bucket, prefix)

    @staticmethod
    def get_object_(bucket: oss2.api.Bucket, key: str):
        try:
            print(f"get_object: {key}")
            return bucket.get_object(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def get_object(self, key: str):
        return self.get_object_(self.bucket, key)

    @staticmethod
    def put_object_(bucket: oss2.api.Bucket, key: str, content):
        if not content:
            return
        try:
            print(f"put_object: {key}")
            bucket.put_object(key, content)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def put_object(self, key: str, content):
        self.put_object_(self.bucket, key, content)

    @staticmethod
    def object_exists_(bucket: oss2.api.Bucket, key: str):
        print(f"object_exists: {key}")
        return bucket.object_exists(key)

    def object_exists(self, key: str):
        return self.object_exists_(self.bucket, key)

    @staticmethod
    def get_object_meta_(bucket: oss2.api.Bucket, key: str):
        try:
            print(f"get_object_meta: {key}")
            return bucket.get_object_meta(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def get_object_meta(self, key: str):
        return self.get_object_meta_(self.bucket, key)

    @staticmethod
    def head_object_(bucket: oss2.api.Bucket, key: str):
        try:
            print(f"head_object: {key}")
            return bucket.head_object(key)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def head_object(self, key: str):
        return self.head_object_(self.bucket, key)

    @staticmethod
    def last_modified_(bucket: oss2.api.Bucket, key: str):
        try:
            return OSSOperator.get_object_meta_(bucket, key).headers['Last-Modified']
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def last_modified(self, key: str):
        return self.last_modified_(self.bucket, key)

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

    @property
    def oss_key(self):
        raise NotImplementedError()

    def execute(self, context):
        self.put_object(key=self.oss_key, content=self.content)
        return self.oss_key
