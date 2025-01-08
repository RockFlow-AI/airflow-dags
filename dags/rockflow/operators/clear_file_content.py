from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Hashable

import oss2
import pandas as pd
from rockflow.operators.oss import OSSOperator


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
        self.clear_object_(self.avatar_bucket, self.from_key)

    def execute(self, context):
        self.clear_from_key_file()

#     @staticmethod
#     def clear_object_(bucket: oss2.api.Bucket, key: str):
#         try:
#             print(f"Clearing file content for key: {key}")
#             result = bucket.put_object(key, "")
#             print(f"Put object result: {result}")
#         except Exception as e:
#             raise AirflowException(f"Errors: {e}")
#
#     def clear_object(self, key: str):
#         self.clear_object_(self.avatar_bucket, key)
