def join_map(first, second):
    first_dict = {
        item["symbol"]: item for item in first
    }
    second_dict = {
        item["symbol"]: item for item in second
    }
    result = {}
    for k, v in second_dict.items():
        second_map = {
            k: v for k, v in v.items() if v
        }
        if k in first_dict:
            first_map = {
                k: v for k, v in first_dict[k].items() if v
            }
            result[k] = {**first_map, **second_map}
        else:
            result[k] = second_map
    return result
