from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Hashable

import oss2
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
        print(f"Clearing file content for key: {self.from_key}")
        # 修复调用 put_object 的代码
        self.put_object(self.from_key, "")

    def execute(self, context):
        self.clear_from_key_file()
