import os

from rockflow.common.downloader import Downloader


class Logo(Downloader):
    def __init__(self, symbol: str, prefix: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbol = symbol
        self.prefix = prefix

    @property
    def url(self):
        raise NotImplementedError()

    @property
    def type(self):
        return "png"

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.prefix}_{self.lowercase_class_name}",
            f"{self.symbol}.{self.type}"
        )


class Public(Logo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return f"https://universal.hellopublic.com/companyLogos/{self.symbol}@3x.png"


class Etoro(Logo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return f"https://etoro-cdn.etorostatic.com/market-avatars/{self.symbol}/150x150.png"
