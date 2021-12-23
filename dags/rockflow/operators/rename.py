from rockflow.common.rename import symbol_match, symbol_rename
from rockflow.operators.oss import OSSDeleteOperator, OSSRenameOperator


class DeleteInvalidOss(OSSDeleteOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def to_delete(self, obj):
        return symbol_match(obj.key)


class RenameOss(OSSRenameOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def dest_name(self, obj):
        return symbol_rename(obj.key)
