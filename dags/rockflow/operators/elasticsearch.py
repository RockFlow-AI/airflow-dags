import json
from typing import Dict

from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

from rockflow.operators.oss import OSSOperator


class ElasticsearchOperator(OSSOperator):
    def __init__(
            self,
            oss_source_key: str,
            elasticsearch_index_name: str,
            elasticsearch_index_setting: str,
            elasticsearch_conn_id: str = 'elasticsearch_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oss_source_key = oss_source_key
        self.elasticsearch_index_name = elasticsearch_index_name
        self.elasticsearch_index_setting = elasticsearch_index_setting
        self.elasticsearch_conn_id = elasticsearch_conn_id

        self.es_hook = ElasticsearchHook(elasticsearch_conn_id=self.elasticsearch_conn_id)

    @property
    def client(self):
        return self.es_hook.get_conn().es

    def create_index(self):
        return self.client.indices.create(index=self.elasticsearch_index_name, body=self.elasticsearch_index_setting)

    def exists_index(self):
        return self.client.indices.exists(index=self.elasticsearch_index_name)

    def delete_index(self):
        return self.client.indices.delete(index=self.elasticsearch_index_name)

    def delete_and_create(self):
        if self.exists_index():
            self.delete_index()
        self.create_index()

    def refresh_index(self):
        return self.client.indices.refresh(index=self.elasticsearch_index_name)

    def add_one_doc(self, id, doc):
        return self.client.index(index=self.elasticsearch_index_name, id=id, body=json.dumps(doc, ensure_ascii=False))

    def execute(self, context: Dict) -> None:
        self.log.info(f"Loading {self.oss_source_key} to Elasticsearch...")
        self.log.info(f"{self.elasticsearch_index_setting}")
