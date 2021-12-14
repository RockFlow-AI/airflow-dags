import pandas as pd


def merge_data_frame(data_frame_list):
    result = None
    for data_frame_item in data_frame_list:
        if result is None:
            result = data_frame_item
        else:
            result = pd.concat([result, data_frame_item], axis=0, ignore_index=True)
    return result
