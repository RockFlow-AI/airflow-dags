import os

from rockflow.operators.oss import OSSSaveOperator


class DownloadOperator(OSSSaveOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def instance(self):
        return self.downloader_cls(
            proxy=self.proxy
        )

    @property
    def oss_key(self):
        return os.path.join(self.key, self.instance.file_name)

    @property
    def downloader_cls(self):
        raise NotImplementedError()

    @property
    def content(self):
        r = self.instance.get()
        if self.instance.check(r):
            return r.content
