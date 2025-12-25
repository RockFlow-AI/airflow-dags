import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

TICKERS = [
    "XPEV","BABA","AAPL","PDD","BIDU","TCEHY","JD","TCOM","LI","NIO","TSM",
    "LTRY","APDN","BGMS","BTQ","ADAP","PLTR","AI","UPST","PATH","SMCI","INOD",
    "BBAI","SOUN","SYM","VRNT","MCARY","BRCC","PEW","DTE","CKPT","VXRT","DPRO",
    "SERV","CEP","DRUG","QNTM","TLN","WOLF","COIN","ASML","NU","RDDT","AVGO",
    "AMD","MU","HOOD","PINS","TTD","DDOG","SNPS","CRWD","ARM","GOOG","META",
    "MSFT","NVDA","TSLA"
]

with DAG(
    "stocks_seo_daily_generate",
    catchup=False,
    start_date=pendulum.datetime(2025, 12, 25, tz="Asia/Shanghai"),
    schedule_interval="0 7 * * *",
    default_args={
        "owner": "tanqiwen",
        "depends_on_past": False,
        "retries": 0,
    },
) as dag:
    generate_stocks_seo = SimpleHttpOperator(
        task_id="generate_stocks_seo",
        method="POST",
        http_conn_id="stocks-seo",
        endpoint="/generate",
        headers={"Content-Type": "application/json"},
        data=f'{{"tickers": {TICKERS}, "force": false}}',
        response_check=lambda response: response.json().get("success") is True,
        log_response=True,
    )
