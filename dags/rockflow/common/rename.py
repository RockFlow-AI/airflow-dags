from pathlib import Path


def symbol_match(key):
    return key != symbol_rename(key)


def new_symbol(symbol):
    if symbol.isnumeric():
        return "%05d" % int(symbol)
    else:
        return symbol


def new_symbol_with_market(name):
    if name.endswith(".SS"):
        name = name.replace(".SS", ".SH")
    i = name.rfind('.')
    if 0 < i < len(name) - 1:
        symbol = name[:i]
    else:
        symbol = name
    return name.replace(symbol, new_symbol(symbol))


def symbol_rename(key):
    old_symbol = Path(key).stem
    new_symbol = new_symbol_with_market(old_symbol)
    return key.replace(old_symbol, new_symbol)
