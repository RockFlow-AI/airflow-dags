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
    def downloader_cls(self):
        raise NotImplementedError()

    @property
    def content(self):
        return self.instance.get().content
