from typing import List, Optional, Dict

import pandas as pd


def merge_data_frame(data_frame_list: Optional[List[pd.DataFrame]], concat_axis=0, concat_ignore_index=True) -> Optional[pd.DataFrame]:
    result = None
    for data_frame_item in data_frame_list:
        if result is None:
            result = data_frame_item
        else:
            result = pd.concat([result, data_frame_item],
                               axis=concat_axis, ignore_index=concat_ignore_index)
    return result


def map_frame(df: Optional[pd.DataFrame], mapper: Dict[str, str]) -> Optional[pd.DataFrame]:
    result = pd.DataFrame(columns=[v for _, v in mapper.items()])
    for k, v in mapper.items():
        result[v] = df[k]
    return result
