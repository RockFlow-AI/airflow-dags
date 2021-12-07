import pandas as pd


class DataFrameMerger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def merge(self, data_frame_list):
        result = None
        for data_frame_item in data_frame_list:
            if result is None:
                result = data_frame_item
            else:
                result = pd.concat([result, data_frame_item], axis=0, ignore_index=True)
        return result
