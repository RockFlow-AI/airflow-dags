from typing import Any, Hashable
import oss2
from rockflow.operators.oss import OSSOperator
from airflow.exceptions import AirflowException


class ClearFileContentOperator(OSSOperator):
    def __init__(self,
                 from_key: str,
                 avatar_bucket_name: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.avatar_bucket_name = avatar_bucket_name

    @property
    def avatar_bucket(self) -> oss2.api.Bucket:
        return self.oss_hook.get_bucket(self.avatar_bucket_name)

    def clear_from_key_file(self):
        print(f"Clearing file content for key: {self.from_key} in bucket: {self.avatar_bucket_name}")
        self.put_object(self.from_key, "")

    def execute(self, context):
        self.clear_from_key_file()

    @staticmethod
    def put_object_(bucket: oss2.api.Bucket, key: str, content):
        try:
            print(f"put_object: {key}")
            result = bucket.put_object(key, content)  # 上传文件内容
            print(f"Put object result: {result}")
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    def put_object(self, key: str, content):
        self.put_object_(self.avatar_bucket, key, content)
