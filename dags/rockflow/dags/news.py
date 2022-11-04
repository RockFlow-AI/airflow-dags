import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

popular_stock_news_refresh = DAG(
    "popular_stock_news_refresh",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 10,16 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='popular_stock_news_refresh',
    method='POST',
    http_conn_id='flow-news',
    endpoint='/news/inner/headlines/reload?popular=true',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=popular_stock_news_refresh,
)

unpopular_stock_news_refresh = DAG(
    "unpopular_stock_news_refresh",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 10 * * 3',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='unpopular_stock_news_refresh',
    method='POST',
    http_conn_id='flow-news',
    endpoint='/news/inner/headlines/reload?popular=true',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=unpopular_stock_news_refresh,
)
