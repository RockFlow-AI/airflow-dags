import httpx


class WebHook(object):
    def __init__(
            self,
            url,
    ):
        self.url = url

    def text(self, content):
        return {
            "msg_type": "text",
            "content": {
                "text": content
            }
        }

    def send(self, content):
        r = httpx.post(
            url=self.url,
            json=self.text(content)
        )
        self.log.info(
            f"status_code: {r.status_code}, url: {self.url}, text: {content}")
        return r
