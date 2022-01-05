import httpx
from stringcase import snakecase
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from rockflow.common.header import user_agent_headers


class Downloader(object):
    def __init__(
            self,
            proxy=None,
    ):
        self._proxy = proxy

    @property
    def snakecase_class_name(self):
        return snakecase(self.__class__.__name__)

    @property
    def lowercase_class_name(self):
        return self.__class__.__name__.lower()

    @property
    def type(self):
        raise NotImplementedError()

    @property
    def file_name(self):
        return f"{self.snakecase_class_name}.{self.type}"

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

    async def async_get(self) -> httpx.Response:
        print(f"url: {self.url}, proxy: {self.proxy}")
        async with httpx.AsyncClient(
                proxies=self.proxy,
        ) as client:
            r = await client.get(
                url=self.url,
                params=self.params,
                headers=self.headers,
                timeout=self.timeout
            )
            print(
                f"status_code: {r.status_code}, url: {self.url}, params: {self.params}")
            return r

    def check(self, r: httpx.Response) -> bool:
        return r.status_code == 200

    @retry(retry=retry_if_exception_type(BaseException),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           stop=stop_after_attempt(3))
    def get(self) -> httpx.Response:
        print(
            f"url: {self.url}, params: {self.params}, headers: {self.headers}, timeout: {self.timeout}, proxy: {self.proxy}")
        r = httpx.get(
            url=self.url,
            params=self.params,
            proxies=self.proxy,
            headers=self.headers,
            timeout=self.timeout
        )
        check = self.check(r)
        print(
            f"status_code: {r.status_code}, url: {self.url}, params: {self.params}, check: {check}")
        if check:
            return r
