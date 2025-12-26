import json
import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 支持的语言列表
LANGUAGES = ["zh-Hans", "zh-Hant", "en", "es"]

# 合并了原始列表和新增的长列表，并进行了去重处理
TICKERS = list(set([
    # 原有 Tickers
    "XPEV","BABA","AAPL","PDD","BIDU","TCEHY","JD","TCOM","LI","NIO","TSM",
    "LTRY","APDN","BGMS","BTQ","ADAP","PLTR","AI","UPST","PATH","SMCI","INOD",
    "BBAI","SOUN","SYM","VRNT","MCARY","BRCC","PEW","DTE","CKPT","VXRT","DPRO",
    "SERV","CEP","DRUG","QNTM","TLN","WOLF","COIN","ASML","NU","RDDT","AVGO",
    "AMD","MU","HOOD","PINS","TTD","DDOG","SNPS","CRWD","ARM","GOOG","META",
    "MSFT","NVDA","TSLA",
    
    # 新增 Tickers
    "AAOI", "ABNB", "ACHR", "ADBE", "ADP", "AGNC", "ALAB", "ALB", "ALK", "AMAT", 
    "AMC", "AMC2", "AMKR", "AMSC", "AMZN", "APLD", "ARAY", "ARE", "ARQQ", "ASTS", 
    "AVAV", "BABA2", "BEKE", "BILI", "BKKT", "BMNR", "BN", "BPOP", "BRK.B", "BTBT", 
    "CHAU", "CRCL", "CRDO", "CRM", "CRWV", "CVNA", "CVX", "DELL", "DIS", "DJT", 
    "DLTR", "DOCU", "DUOL", "EDU", "EH", "ENVX", "EOSE", "ETHA", "ETHU", "EVEX", 
    "FBTC", "FCX", "FFAI", "FFIE3", "FIGR", "FUTU", "GBTC", "GME", "GOOGL", "GOTU", 
    "GRAB", "GTLB", "HIMS", "HON", "HSAI", "IBKR", "IBM", "INTC", "IONQ", "IQ", 
    "IREN", "JOBY", "JPM", "KNX", "KO", "KSPI", "LAES", "LCID", "LEU", "LEVI", 
    "LILMF", "LITM", "LLY", "LLYVK", "LTBR", "LULU", "LYFT", "MANH", "MARA", "MP", 
    "MRVL", "MSTR", "NBIS", "NET", "NFLX", "NIU", "NKE", "NTES", "NVO", "NVTS", 
    "OKLO", "OLLI", "ONDS", "OPEN", "OPEN1", "ORCL", "OSCR", "OUST", "PFE", "PYPL", 
    "QBTS", "QCOM", "QFIN", "QQQ", "QS", "QUBT", "RBLX", "RCAT", "RDW", "RGTI", 
    "RR", "RTX", "SBET", "SE", "SES", "SHOP", "SLM", "SMR", "SNAP", "SNOW", "SPCE", 
    "SPXC", "SPXL", "SRAD", "TAL", "TEM", "TGT", "TIGR", "TME", "U", "UAVS", "UBER", 
    "UEC", "UNH", "UUUU", "V", "VIPS", "VST", "VSTM", "WMT", "XYZ", "TTWO", "OXY", 
    "SNDL", "MAA", "FUBO", "ROKU", "MAXN", "RIVN", "CMCSA", "ADM", "PODD", "ZI", 
    "ENPH", "RKLB", "MDLZ", "SAP", "LKNCY", "RYCEY"
]))

# 按照字母顺序排序
TICKERS.sort()

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
        data=json.dumps({
            "tickers": TICKERS,
            "force": False,
            "languages": LANGUAGES  # 四种语言
        }),
        response_check=lambda response: response.json().get("success") is True,
        log_response=True,
    )
