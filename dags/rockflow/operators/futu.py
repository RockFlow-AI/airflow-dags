from typing import Any

from operators.oss import OSSOperator


class FutuOperator(OSSOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Any):
        raise NotImplementedError()
