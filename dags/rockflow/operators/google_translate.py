from multiprocessing.pool import ThreadPool as Pool
from typing import Any

import pandas as pd
from googletrans import Translator
from rockflow.operators.const import DEFAULT_POOL_SIZE
from rockflow.operators.oss import OSSOperator
from tqdm import tqdm


class GoogleTranslateOperator(OSSOperator):
    def __init__(self,
                 path: str = 'appstore_reviews',
                 files: [] = None,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.files = files
        self.pool_size = pool_size

    def translate(self, file: str):
        review_lang = []
        translated_review = []
        df = pd.read_json(self.get_object(f"{self.path}/{file}"), orient='table')

        trans = Translator(proxies=self.proxy)

        for i in tqdm(range(len(df))):
            text = df['review'][i]
            translated_text = trans.translate(text, dest='en')
            review_lang.append(translated_text.src)
            translated_review.append(translated_text.text)

        df['review_lang'] = review_lang
        df['translated_review'] = translated_review

        json = df.to_json(orient='table')
        self.put_object(key=self.oss_key, content=json)

    def execute(self, context: Any):
        with Pool(self.pool_size) as pool:
            pool.map(
                lambda x: self.translate(x), self.files
            )
