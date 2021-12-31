import os
from multiprocessing.pool import ThreadPool as Pool
from tempfile import NamedTemporaryFile
from typing import Any

from airflow.providers.ssh.hooks.ssh import SSHHook

from rockflow.operators.oss import OSSOperator


class SftpToOssOperator(OSSOperator):
    def __init__(
            self,
            prefix: str,
            work_dir: str,
            ssh_conn_id: str = 'ssh_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.work_dir = work_dir
        self.ssh_conn_id = ssh_conn_id
        self.pool_size = 5
        self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

    @property
    def sftp_client(self):
        return self.ssh_hook.get_conn().open_sftp()

    def sync_one(self, file):
        filename = file.filename
        obj_path = os.path.join(self.prefix, filename)
        if self.object_exists(obj_path):
            return
        with NamedTemporaryFile("w") as f:
            self.sftp_client.get(
                os.path.join(self.work_dir, filename),
                f.name
            )
            self.put_object_from_file(
                obj_path,
                f.name
            )

    def execute(self, context: Any):
        with Pool(self.pool_size) as pool:
            pool.map(
                lambda file: self.sync_one(
                    file), self.sftp_client.listdir_attr(self.work_dir)
            )
