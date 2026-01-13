import json
import pendulum
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 支持的语言列表
LANGUAGES = ["zh-Hans", "zh-Hant", "en", "es"]

# 合并了原始列表和新增的长列表，并进行了去重处理
TICKERS_RAW = [
    "SPY","TSLA","NVDA","QQQ","AAPL","GOOGL","MSFT","META","GLD","AMZN","SLV","MU","GOOG","IWM","AVGO","PLTR","BABA","WMT","SNDK","AMD","ORCL","VOO","IVV","V","INTC","TSM","JPM","CRWV","COF","TQQQ","NFLX","LQD","TLT","COST","SOXL","APP","DIA","EEM","IREN","MSTR","GDX","LLY","BAC","AXP","JNJ","RSP","UNH","EVTV","FXI","LRCX","C","XLF","QCOM","HD","SMH","XLP","IBIT","BRK.B","GS","SQQQ","AMAT","HOOD","VTI","MA","LITE","TMO","HYG","RKLB","XLV","APLD","BA","ASML","XOM","XLK","PG","GEV","PEP","NBIS","SOXX","BE","KWEB","LMT","ONDS","CVX","WDC","CSCO","EFA","WFC","SGOV","APH","AGQ","AAL","KO","COIN","ABBV","NOW","ALNY","GE","XLE","SOFI","ASTS","CRM","PFE","QQQM","XBI","VST","OKLO","VZ","SATS","TXN","DIS","KLAC","IBM","XLI","TSLL","FCX","SNPS","CMCSA","CAT","MRK","NVO","ADBE","HON","ACN","SPOT","PYPL","TEM","KRE","COHR","XLY","ANET","UBER","PDD","IEMG","MS","VEA","RTX","BMNR","ISRG","TMUS","IWD","IEFA","INTU","REGN","LIN","CVNA","AZN","IWF","EQT","PM","JCI","AGG","MCD","STX","COP","EQIX","DASH","ADI","PSLV","T","BKNG","GDXJ","IAU","NEM","SHOP","SPYM","QBTS","NKE","GILD","SCHW","CRWD","NEE","FLUT","SLB","LOW","RBLX","EWY","AFRM","MELI","LULU","TJX","DHR","USB","DDOG","IONQ","MRVL","CEG","RKT","AZO","CME","MTSI","BITO","HIMS","MP","PANW","VTV","AMGN","NOC","BLK","ZTS","DXCM","MDT","KVUE","ALAB","DE","EMB","DAL","TT","UNP","SYF","F","RVMD","SMCI","ABVX","BIDU","BMY","SNOW","BND","VCLT","FISV","ALL","CRDO","INSM","GLDM","XLU","KMB","ETN","ORLY","VRT","ICE","PGR","SCHD","DHI","SYK","VXUS","DUOL","BSX","KKR","ANF","ABT","ARKK","B","D","XLC","IJH","SBUX","RGTI","MDLZ","RDDT","DELL","NVDL","TGT","AIG","HL","WBD","PNC","VRTX","SHW","XLB","AKAM","IEF","MO","GM","CARR","VLO","AEM","IWB","TFC","KTOS","CVS","BDSX","EWZ","VCIT","DG","ALB","CMG","TZA","AA","CDE","AVAV","VWO","CRCL","BIL","LEN","SPGI","RIVN","GD","EA","ON","NXPI","MBB","ETHA","QID","CL","URI","FIX","TER","GIS","MMM","AG","IGSB","XRT","SPIB","CSGP","ACHR","VUG","EWJ","CIEN","MDY","HBAN","SOXS","SMR","MCHP","ROKU","DRI","PYLD","CLS","FSLR","U","MUB","SO","IYR","TEVA","LHX","VT","BX","SCCO","EMXC","LVS","GLW","ARM","HLT","VIG","SIVR","RY","ITB","CDNS","AJG","RCL","IJR","COPX","STZ","JD","PSTG","PLNT","CIFR","PL","EOSE","AMT","UPS","TRV","NU","CCL","ABNB","JPIE","WMB","SOUN","HWM","BK","MDB","NET","TNA","WELL","CSX","MAR","PH","CI","VGT","MMC","WM","SIL","CRML","EL","BLDR","CRH","KR","XME","SPHQ","PAAS","BRK.A","CPNG","HAL","XHB","CCJ","FDX","ACWI","FND","WDAY","KEY","XEL","VCSH","ZSL","OPEN","NTRA","MCK","MARA","USO","BKR","SE","TTD","NCLH","CB","EMLC","AEP","MGK","PSA","ARGX","CMI","ROST","DLTR","BBAI","APD","CPRT","TDG","ADP","POOL","UAL","DUST","UPST","MNST","MSI","SCHG","BBIO","PHYS","LYV","COMP","TEL","O","JPST","KGC","TEAM","AON","ELV","MPC","LUV","WULF","KMI","IDXX","EXPE","CHWY","EXE","XYZ","EBAY","FITB","SPXL","VEEV","BBY","DUK","MCO","NVD","ITOT","HCA","APO","JAAA","COR","OXY","VALE","XOP","RIOT","PHM","EOG","XPEV","RDW","AEO","JOBY","HSY","XLRE","FICO","RIO","URBN","MPWR","RF","IGV","PCAR","FIVE","FAST","ADSK","FUTU","PSX","SAP","EMR","FIGR","UPRO","JEPQ","RGLD","IVW","SNAP","VTWO","ODFL","PWR","LMND","CHKP","TD","PATH","CTSH","HPE","MRNA","CWAN","BApA","FBTC","PSQ","SHEL","FTAI","SILJ","BHP","UNG","VYM","ENPH","CTAS","CAH","CLSK","ACI","FTNT","KHC","TSCO","BNDX","STT","AXON","NRG","SYY","CAVA","WCN","NDAQ","IBB","AIA","VNQ","ZS","VGSH","TOL","FANG","KDP","AMP","DKNG","FN","VTEB","TTWO","CBOE","NVTS","VGK","TXRH","APTV","RMD","LNG","M","PEN","ULTA","KKRpD","BURL","SPMO","MET","BOIL","QLD","DLR","WPM","TSLS","INDA","BDX","SCHX","IWO","FIS","OKE","LVLU","ROP","NSC","JBL","PINS","COO","IOT","SHAK","EME","SN","HPQ","EW","JEPI","CORZ","ITW","QQQI","SH","JNK","PLTD","CART","HUBS","PLD","OMC","USHY","TSLQ","REMX","VMC","CFG","TOST","SDY","Q","VICI","NUE","AME","IWR","SWKS","ITA","EXC","WGS","UGL","QXO","NBIX","OKTA","FERG","ASND","NVR","SHV","CBRE","NXUS","BILI","GH","MOH","CTVA","TSLZ","CHD","UDOW","MTB","MLM","SPG","DECK","TSLG","IRE","DPZ","NXT","CGDV","BP","TWLO","CHTR","EFX","IRTC","LYB","EWT","TSDD","NIO","BITX","PPLT","VEU","CNC","GAP","BWXT","ATEC","AMKR","RWM","CLX","GPN","DVN","HII","ILMN","RSG","NLY","SPLV","BIIB","URA","BEAM","GDS","LEU","SHLD","YUM","TECK","ASHR","BRO","CELH","GBTC","CTRA","PAYX","ECL","ARES","AYI","ET","AGNC","HYMC","ONON","TLN","SPYG","INFY","SPTL","CF","GTLB","W","PPG","PLUG","CPB","UUUU","CFLT","WSM","ENB","LYFT","IQV","PFGC","BAX","THC","BSV","GWRE","JBHT","AWK","MCHI","OTIS","ESLT","ES","PEG","LH","BKLN","CCI","TSN","AXTI","NTAP","DKS","FFIV","HUT","VRSK","RBRK","RJF","STM","PCG","ZM","USAR","XYL","NVS","PRU","AR","GGLL","BN","WST","SFM","SW","MKC","TDY","CAG","TPR","BLD","WY","VO","AU","USFR","CHRW","DAY","ACGL","CRS","PTCT","GDXU","DOW","VGIT","ZBRA","FNV","KBWB","AXSM","SITE","IBKR","ROK","DOV","WING","TXT","LUNR","HUM","WEC","MSTX","FYBR","ACWX","QUAL","XAR","VIK","ETHU","XPO","FLIN","SCEP","ALLY","ED","IBN","HQY","AVB","SPXU","SCHH","IT","CASY","CNQ","DAVE","STRL","EQR","BOXX","ENTG","UTHR","LDOS","SRE","WIX","DT","MAGS","SGOL","INCY","SCHF","SSNC","CADE","YINN","SPXS","FIG","IVZ","IVE","BIV","CMS","SHY","NGD","CACI","KEYS","STLD","USFD","PKG","RPM","IR","EXEL","SNCY","GMED","FOXA","ETSY","DD","INVH","TSEM","FNGD","TMF","ONC","BOOT","GWW","QUBT","OC","CLF","WLK","STRC","FCNCA","DEO","MOS","SBAC","BTG","EXPD","IP","AMCR","BMNU","PODD","CNP","VUSB","AMRZ","AMH","SU","BFH","ADM","BIRK","TIP","CVE","FLEX","VSCO","DOC","GEHC","ZETA","GDDY","GFI","OIH","SJM","EAT","ALGT","CPAY","PPL","TRU","TRGP","VSNT","EFV","RACE","JEF","UAMY","ARWR","A","PNR","ESS","FOLD","FENY","NVT","BAH","ETR","BMO","OEF","MEDP","BRKR","Z","PBF","DOCU","UVXY","GPC","GNRC","GRMN","CRL","NUGT","NTR","SPYV","AFL","POWL","NOK","SCNM","TAP","HUBB","PTC","NTNX","SPYI","TROW","HIG","NI","CYTK","IYT","ICLR","FAS","EXR","BITB","RL","SNY","MTD","MGM","LAC","BF.B","VXX","LPX","EXAS","ATO","MTZ","EVRG","VTR","FWONK","DVAX","IXUS","IYW","SIDU","EIX","RELX","RH","WRB","ALGN","JLL","AER","EXK","ATI","SMMT","EXP","MDGL","QS","UHS","ETH","HEI","FBIN","TCOM","SEDG","AI","ZBH","IWY","WSO","HAS","SCHB","WTW","FTI","ECG","SSO","CW","UEC","OSCR","JAZZ","PBUS","TFX","VONG","PAYC","BITF","KRMN","IBP","CVCO","HSIC","MNDY","MTN","IEI","KOLD","SLNO","WYNN","BR","DYNF","DGRO","MDLN","SCMC","USIG","VYMI","CPT","CG","LSCC","UL","ZIM","SEE","BROS","TYL","DOCN","PBR","AMDL","OM","STLA","PRAX","RCAT","LABU","IWN","SRPT","KLAR",
]

# 去重并排序
TICKERS = list(set(TICKERS_RAW))
TICKERS.sort()

with DAG(
    dag_id="stocks_seo_daily_generate",
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
        # 建议增加超时设置，因为处理1000个ticker可能很慢
        extra_options={"timeout": 300}, 
        data=json.dumps({
            "tickers": TICKERS,
            "force": False,
            "languages": LANGUAGES  # 四种语言
        }),
        response_check=lambda response: response.json().get("success") is True,
        log_response=True,
    )
