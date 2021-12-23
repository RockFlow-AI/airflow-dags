from pathlib import Path


def symbol_match(key):
    return key != symbol_rename(key)


def new_symbol(symbol):
    if symbol.isnumeric():
        return "%05d" % int(symbol)
    else:
        return symbol


def new_symbol_with_market(name):
    i = name.rfind('.')
    if 0 < i < len(name) - 1:
        symbol = name[:i]
    else:
        symbol = name
    return name.replace(symbol, new_symbol(symbol))


def symbol_rename(key):
    return new_symbol_with_market(Path(key).stem)
