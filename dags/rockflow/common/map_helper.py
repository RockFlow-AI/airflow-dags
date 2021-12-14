def join_map(first, second):
    result = {}
    for k, v in second.items():
        second_map = {
            k: v for k, v in v.items() if v
        }
        if k in first:
            first_map = {
                k: v for k, v in first[k].items() if v
            }
            result[k] = {**first_map, **second_map}
        else:
            result[k] = second_map
    return result


def join_list(first, second):
    first_dict = {
        item["symbol"]: item for item in first
    }
    second_dict = {
        item["symbol"]: item for item in second
    }
    return join_map(first_dict, second_dict)
