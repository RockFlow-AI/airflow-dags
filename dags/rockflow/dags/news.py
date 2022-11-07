import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

popular_stock_news_refresh_am = DAG(
    "popular_stock_news_refresh_am",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 10 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='popular_stock_news_refresh_am',
    method='POST',
    http_conn_id='flow-news',
    endpoint='/news/inner/headlines/reload?popular=true',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=popular_stock_news_refresh_am,
)

popular_stock_news_refresh_pm = DAG(
    "popular_stock_news_refresh_pm",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 16 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='popular_stock_news_refresh_pm',
    method='POST',
    http_conn_id='flow-news',
    endpoint='/news/inner/headlines/reload?popular=true',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=popular_stock_news_refresh_pm,
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
    endpoint='/news/inner/headlines/reload?popular=false',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=unpopular_stock_news_refresh,
)
