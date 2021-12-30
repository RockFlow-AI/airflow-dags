def is_none_us_symbol(symbol: str) -> bool:
    return symbol.endswith(".HK") or symbol.endswith(".SZ") or symbol.endswith(".SH")


def is_us_symbol(symbol: str) -> bool:
    return not is_none_us_symbol(symbol)
