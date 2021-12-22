from pathlib import Path

from rockflow.operators.oss import OSSDeleteOperator, OSSRenameOperator


def symbol_match(key):
    return key != symbol_rename(key)


def new_symbol(symbol):
    if symbol.isnumeric():
        return "%05d" % int(symbol)
    else:
        return symbol


def symbol_rename(key):
    name = Path(key).stem
    i = name.rfind('.')
    if 0 < i < len(name) - 1:
        symbol = name[:i]
    else:
        symbol = name
    return key.replace(symbol, new_symbol(symbol))


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
