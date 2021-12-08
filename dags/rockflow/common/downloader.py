import requests as _requests

from rockflow.common.header import user_agent_headers


class Downloader(object):
    def __init__(
            self,
            session=None,
            proxy=None,
    ):
        self._session = session or _requests
        self._proxy = proxy

    @property
    def session(self):
        return self._session

    @property
    def url(self):
        raise NotImplementedError()

    @property
    def params(self):
        return {}

    @property
    def proxy(self):
        return self._proxy

    @property
    def headers(self):
        return user_agent_headers

    @property
    def timeout(self):
        return 5

    def get(self) -> _requests.Response:
        print(f"url: {self.url}, proxy: {self.proxy}")
        r = self._session.get(
            url=self.url,
            params=self.params,
            proxies=self.proxy,
            headers=self.headers,
            timeout=self.timeout
        )
        print(f"status_code: {r.status_code}, url: {self.url}, params: {self.params}")
        return r
