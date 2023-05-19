import json
import os
from io import BytesIO
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from stringcase import snakecase

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn, FutuCompanyProfile
from rockflow.common.map_helper import join_map, join_list
from rockflow.operators.const import DEFAULT_POOL_SIZE, GLOBAL_DEBUG
from rockflow.operators.elasticsearch import ElasticsearchOperator
from rockflow.operators.mysql import MysqlToOssOperator
from rockflow.operators.mysql import OssToMysqlOperator

from rockflow.operators.oss import OSSSaveOperator, OSSOperator


class FutuBatchOperator(OSSOperator):
    def __init__(self,
                 from_key: str,
                 key: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.key = key

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))

    def object_not_update_for_a_week(self, key: str) -> bool:
        if not self.object_exists(key):
            return True
        elif GLOBAL_DEBUG:
            return False
        try:
            return GmtDatetimeCheck(
                self.last_modified(key), weeks=1
            ).check
        except Exception as e:
            self.log.error(f"futu batch operator error: {str(e)}")
            return True

    def save_one(self, line: pd.Series, cls):
        obj = cls(
            symbol=line['rockflow'],
            futu_ticker=line['futu'],
            prefix=self.key,
            proxy=self.proxy
        )
        if self.object_not_update_for_a_week(obj.oss_key):
            r = obj.get()
            if not r:
                return
            self.put_object(obj.oss_key, r.content)

    @property
    def cls(self):
        raise NotImplementedError()

    @property
    def oss_key(self):
        return f"{self.key}_{snakecase(self.cls.__name__)}"

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols}")
        self.symbols.apply(
            self.save_one,
            axis=1,
            args=(self.cls,)
        )
        return self.oss_key


class FutuBatchOperatorCn(FutuBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileCn


class FutuBatchOperatorEn(FutuBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileEn


class FutuBatchOperatorCnDebug(FutuBatchOperatorCn):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class FutuBatchOperatorEnDebug(FutuBatchOperatorEn):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class FutuExtractHtml(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(
            self,
            from_key: str,
            pool_size: int = DEFAULT_POOL_SIZE,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.pool_size = pool_size

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.key}_{self.snakecase_class_name}_{self.from_key}",
            f"{self.snakecase_class_name}.json"
        )

    def symbol(self, obj):
        return Path(obj.key).stem

    def extract_data(self, obj):
        if obj.is_prefix():
            return
        return FutuCompanyProfile.extract_data(
            self.get_object(obj.key), self.symbol(obj)
        )

    def iterator(self):
        return self.path_object_iterator(self.from_key)

    @property
    def content(self):
        with Pool(self.pool_size) as pool:
            result = pool.map(
                lambda x: self.extract_data(x), self.iterator()
            )
            return json.dumps(result, ensure_ascii=False)


class FutuExtractHtmlDebug(FutuExtractHtml):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def iterator(self):
        from itertools import islice
        return islice(super().iterator(), 100)


class FutuFormatJson(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.key}_{self.snakecase_class_name}_{snakecase(self.cls.__name__)}",
            f"{self.snakecase_class_name}.json"
        )

    @property
    def cls(self):
        raise NotImplementedError()

    @property
    def content(self):
        result = [
            self.format_(self.cls.language(), i)
            for i in json.load(
                BytesIO(self.get_object(self.from_key).read())
            )
        ]
        return json.dumps(result, ensure_ascii=False)


class FutuFormatJsonCn(FutuFormatJson):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileCn


class FutuFormatJsonEn(FutuFormatJson):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileEn


class JoinMap(OSSSaveOperator):
    template_fields = ["first", "second"]

    def __init__(
            self,
            first: str,
            second: str,
            merge_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.first = first
        self.second = second
        self.merge_key = merge_key

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.key}_{self.snakecase_class_name}",
            f"{self.snakecase_class_name}.json"
        )

    def load_json(self, key):
        return json.load(
            BytesIO(self.get_object(key).read())
        )

    def load_merge_pd(self):
        return pd.read_csv(
            self.get_object(self.merge_key),
            index_col='rockflow',
        ).to_dict('index')

    @property
    def content(self):
        result = join_map(
            join_list(
                self.load_json(self.first),
                self.load_json(self.second)
            ),
            self.load_merge_pd()  # 通过symbol map过滤
        )
        return json.dumps(result, ensure_ascii=False)


class SinkEs(ElasticsearchOperator):
    template_fields = ["from_key"]

    def __init__(
            self,
            from_key: str,
            mapping: Dict[str, str],
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.mapping = mapping

    def load_json(self, key):
        return json.load(
            BytesIO(self.get_object(key).read())
        )

    def check_data(self, data) -> bool:
        return "symbol" in data and "raw" in data and "name_en" in data and "name_zh" in data

    def execute(self, context: Dict) -> None:
        def map_dict(input, mapper: Dict[str, str]):
            result = {}
            for pair in mapper:
                k = pair[0]
                v = pair[1]
                if k in input:
                    result[v] = input[k]
            return result

        self.if_not_exist_and_create()
        for k, v in self.load_json(self.from_key).items():
            res = map_dict(v, self.mapping)
            if not self.check_data(res):
                print("check error: ", res)
                continue
            self.add_one_doc(k, res)


class SinkFutuSearch(SinkEs):
    def __init__(self, **kwargs) -> None:
        if 'mapping' not in kwargs:
            kwargs['mapping'] = [
                ("symbol", "symbol"),
                ("raw", "raw"),
                ("name_en", "name_en"),
                ("name_cn", "name_zh"),
                ("name_zh", "name_zh"),
                ("profile_en", "profile_en"),
                ("profile_cn", "profile_zh"),
                ("market", "market"),
                ("expiry_date", "expiry_date"),
            ]
        super().__init__(**kwargs)


class SinkFutuProfile(OssToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                "symbol": "symbol",
                "raw": "raw",
                "profile_en": "profile_en",
                "profile_cn": "profile_zh",
                "market": "market",
            }
        super().__init__(**kwargs)

    def extract_data(self) -> pd.DataFrame:
        return self.extract_index_dict_to_df(
            self.extract_index_dict()
        )


class SinkFutuProfileDebug(SinkFutuProfile):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def extract_data(self) -> pd.DataFrame:
        return self.extract_index_dict_to_df(
            self.extract_index_dict_from_local_file()
        )


class SinkFutuRenameCompany(MysqlToOssOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'oss_dst_key' not in kwargs:
            kwargs['oss_dst_key'] = self.snakecase_class_name
        super().__init__(**kwargs)
