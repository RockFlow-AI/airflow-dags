def join_map(first, target):
    first_dict = {}
    for item in first:
        first_dict[item["symbol"]] = item

    second_dict = {}
    for item in target:
        second_dict[item["symbol"]] = item

    for k, v in second_dict.items():
        if k in first_dict:
            v.update(first_dict[k])
            first_dict[k] = v

    return first_dict
