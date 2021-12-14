from airflow.models import DAG

from rockflow.dags.const import *
from rockflow.dags.es_settings.search import search_setting
from rockflow.operators.elasticsearch import ElasticsearchOperator

with DAG("es_import", default_args=DEFAULT_DEBUG_ARGS) as company_profile_batch_download:
    ElasticsearchOperator(
        oss_source_key='',
        elasticsearch_index_name='',
        elasticsearch_index_setting=search_setting,
        elasticsearch_conn_id='elasticsearch_default',
        region=DEFAULT_REGION,
        bucket_name=DEFAULT_BUCKET_NAME,
        proxy=DEFAULT_PROXY
    )
