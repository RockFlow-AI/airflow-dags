from typing import List, Optional, Dict

import pandas as pd


def merge_data_frame_by_column(data_frame_list: Optional[List[pd.DataFrame]]) -> Optional[pd.DataFrame]:
    result = None
    for data_frame_item in data_frame_list:
        if result is None:
            result = data_frame_item
        else:
            result = pd.concat([result, data_frame_item],
                               axis=0, ignore_index=True)
    return result


def merge_data_frame_by_index(data_frame_list: Optional[List[pd.DataFrame]]) -> Optional[pd.DataFrame]:
    result = None
    for data_frame_item in data_frame_list:
        if result is None:
            result = data_frame_item
        else:
            result = pd.concat([result, data_frame_item],
                               axis=0, ignore_index=False)
    return result


def map_frame(df: Optional[pd.DataFrame], mapper: Dict[str, str]) -> Optional[pd.DataFrame]:
    result = pd.DataFrame(columns=[v for _, v in mapper.items()])
    for k, v in mapper.items():
        result[v] = df[k]
    return result
