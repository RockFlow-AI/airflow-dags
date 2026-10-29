import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator


ledger_statement_by_daily = DAG(
    "ledger_statement_by_daily",
    catchup=False,
    start_date=datetime(2022, 10, 13, 0, 0),
    schedule_interval='30 8 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 16,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='ledger_statement_by_daily',
    method='PATCH',
    http_conn_id='flow-ledger',
    endpoint='/ledger/inner/statement/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ledger_statement_by_daily,
)

statement_new_by_daily = DAG(
    "statement_new_by_daily",
    catchup=False,
    start_date=datetime(2022, 10, 13, 0, 0),
    schedule_interval='30 8 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 16,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_new_by_daily',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=statement_new_by_daily,
)

statement_sync_ftp_file = DAG(
    "statement_sync_ftp_file",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='50 7,1 * * 1-7',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_ftp_file',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1800},
    dag=statement_sync_ftp_file,
)

statement_sync_delay_file = DAG(
    "statement_sync_delay_file",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='50 9 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_delay_file',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=statement_sync_delay_file,
)

# tradeDetail 兜底
statement_sync_delay_file_1755 = DAG(
    "statement_sync_delay_file_1755",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='10,30,50 10,11 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_delay_file_1755',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=statement_sync_delay_file_1755,
)


option_exercise_report = DAG(
    "option_exercise_report",
    catchup=False,
    start_date=datetime(2022, 12, 5, 0, 0),
    schedule_interval='0 10 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='option_exercise_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/option/exercise/send?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_exercise_report,
)

# 同步文件20:10兜底
statement_sync_file_2010 = DAG(
    "statement_sync_file_2010",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='10 12 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_file_2010',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=statement_sync_file_2010,
)

# 同步文件 T-1
statement_sync_ftp_file_previous = DAG(
    "statement_sync_ftp_file_previous",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='50 7 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_ftp_file_previous',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 1800},
    dag=statement_sync_ftp_file_previous,
)

# 行权11点兜底
statement_sync_file_23 = DAG(
    "statement_sync_file_23",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='50 15 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='statement_sync_file_23',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=statement_sync_file_23,
)

option_exercise_report_2030 = DAG(
    "option_exercise_report_2030",
    catchup=False,
    start_date=datetime(2022, 12, 5, 0, 0),
    schedule_interval='30 12 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='option_exercise_report_2030',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/option/exercise/send?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_exercise_report_2030,
)

sync_us_eod_file = DAG(
    "sync_us_eod_file",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='00 10 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='sync_us_eod_file',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/eod/sync?date={date}&market=US'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=sync_us_eod_file,
)

sync_hk_eod_file = DAG(
    "sync_hk_eod_file",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='00 10 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='sync_hk_eod_file',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/eod/sync?date={date}&market=HK'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=sync_hk_eod_file,
)

# 保证金更新8:30兜底
sync_us_eod_file_2030 = DAG(
    "sync_us_eod_file_2030",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='30 20 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='sync_us_eod_file_2030',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/eod/us/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=sync_us_eod_file_2030,
)

risk_debt_report = DAG(
    "risk_debt_report",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='30 1 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='risk_debt_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/risk/debt/send?date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=risk_debt_report,
)

risk_margin_report = DAG(
    "risk_margin_report",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='00 10 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='risk_margin_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/risk/margin/send?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=risk_margin_report,
)

# 保证金提醒20：30兜底
risk_margin_report_2030 = DAG(
    "risk_margin_report_2030",
    catchup=False,
    start_date=datetime(2023, 2, 21, 0, 0),
    schedule_interval='30 12 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='risk_margin_report_2030',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/risk/margin/send?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=risk_margin_report_2030,
)

US_trade_match_report = DAG(
    "US_trade_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='20,25,40,59 10,11 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='US_trade_match_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/tradeMatch/daily?market=US&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=US_trade_match_report,
)

HK_trade_match_report = DAG(
    "HK_trade_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='20,40,59 10,11 * * 1-7',
    default_args={
        "owner": "caoyunfei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='HK_trade_match_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/tradeMatch/daily?market=HK&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=HK_trade_match_report,
)

US_position_match_report = DAG(
    "US_position_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='20,40,59 10,11 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='US_position_match_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/position/daily?market=US&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=US_position_match_report,
)

HK_position_match_report = DAG(
    "HK_position_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='20,40,59 10,11 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0
    }
)

SimpleHttpOperator(
    task_id='HK_position_match_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/position/daily?market=HK&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=HK_position_match_report,
)

CorpActionClearingCash = DAG(
    "CorpActionClearingCash",
    catchup=False,
    start_date=datetime(2023, 9, 18, 0, 0),
    schedule_interval='00 16 * * 1-7',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 5
    }
)

SimpleHttpOperator(
    task_id='CorpActionClearingCash',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/clearing/handle?file=Cash_InOut&date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=CorpActionClearingCash,
)

CorpActionClearingPosition = DAG(
    "CorpActionClearingPosition",
    catchup=False,
    start_date=datetime(2023, 9, 18, 0, 0),
    schedule_interval='00 16 * * 1-7',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 5
    }
)

SimpleHttpOperator(
    task_id='CorpActionClearingPosition',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/clearing/handle?file=Position_InOut&date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=CorpActionClearingPosition,
)

send_us_daily_statement_email = DAG(
    "send_us_daily_statement_email",
    catchup=False,
    start_date=pendulum.datetime(2024, 5, 25, 0, 0, tz='Asia/Shanghai'),
    schedule_interval='00 20 * * 1-7',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
)

SimpleHttpOperator(
    task_id='send_us_daily_statement_email',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/auto/send/email?market=US&type=1',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=send_us_daily_statement_email,
)

send_hk_daily_statement_email = DAG(
    "send_hk_daily_statement_email",
    catchup=False,
    start_date=pendulum.datetime(2024, 5, 25, 0, 0, tz='Asia/Shanghai'),
    schedule_interval='30 20 * * 1-7',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
)

SimpleHttpOperator(
    task_id='send_hk_daily_statement_email',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/auto/send/email?market=HK&type=1',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=send_hk_daily_statement_email,
)

send_us_monthly_statement_email = DAG(
    "send_us_monthly_statement_email",
    catchup=False,
    start_date=pendulum.datetime(2024, 5, 25, 0, 0, tz='Asia/Shanghai'),
    schedule_interval='00 20 * * 1-7',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
)

SimpleHttpOperator(
    task_id='send_us_monthly_statement_email',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/auto/send/email?market=US&type=2',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=send_us_monthly_statement_email,
)

send_hk_monthly_statement_email = DAG(
    "send_hk_monthly_statement_email",
    catchup=False,
    start_date=pendulum.datetime(2024, 5, 25, 0, 0, tz='Asia/Shanghai'),
    schedule_interval='30 20 * * 1-7',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
)

SimpleHttpOperator(
    task_id='send_hk_monthly_statement_email',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/auto/send/email?market=HK&type=2',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=send_hk_monthly_statement_email,
)

send_hk_position_inout = DAG(
    "send_hk_position_inout",
    catchup=False,
    start_date=pendulum.datetime(2026, 6, 25, 0, 0, tz='Asia/Shanghai'),
    schedule_interval='10,30,50 18,19 * * 1-7',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
)

SimpleHttpOperator(
    task_id='send_hk_position_inout',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/zv/positionInOuts/send?symbols=YFDHKD,YFDUSD&statementDay={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 300},
    dag=send_hk_position_inout,
)


# 老虎（TBHK）日结报表：从老虎 SFTP 拉报表（sync）→ 入库并按产物生成伪装成尊嘉的 TradeDetail / Position / Balance
# 文件（import），供下游 tradeMatch / position / risk / import_statement 等 DAG 照旧读取。TBHK 北京时间 T+1 17:00
# 推送（周五的报表下周一推送，老虎宋祺 2026-10-29 确认），T 日报表落在下一个交易日的文件日 D（方案 B）。两个接口都幂等，所以按尊嘉的做法多轮调：报表没到齐时
# import 只生成齐了的产物，下游接口对没生成的产物返回非 200、等下一轮，不会提前发出 totalCount=0。
# 依赖 flow-statement 20261027-tiger-p0 分支（fileDate 参数）；切到老虎前保持暂停。
# D 在运行时按北京时间取本次调度点（重试跨零点也不变）；手工 trigger 可传 conf {"fileDate": "yyyyMMdd"}。
TIGER_FILE_DATE = ("{{ (dag_run.conf or {}).get('fileDate') "
                   "or data_interval_end.in_timezone('Asia/Shanghai').strftime('%Y%m%d') }}")


def tiger_report_sync_dag(dag_id, schedule_interval, query, retries, retry_delay):
    dag = DAG(
        dag_id,
        catchup=False,
        start_date=pendulum.datetime(2026, 10, 29, 0, 0, tz='Asia/Shanghai'),
        schedule_interval=schedule_interval,
        max_active_runs=1,
        default_args={
            "owner": "guoshizhe",
            "depends_on_past": False,
            "retries": retries,
            "retry_delay": retry_delay
        }
    )
    sync = SimpleHttpOperator(
        task_id='tiger_report_sync',
        method='PATCH',
        http_conn_id='flow-statement',
        endpoint='/inner/tiger/reports/sync?' + query,
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 300},
        dag=dag,
    )
    ingest = SimpleHttpOperator(
        task_id='tiger_report_import',
        method='PATCH',
        http_conn_id='flow-statement',
        endpoint='/inner/tiger/reports/import?' + query,
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 300},
        dag=dag,
    )
    sync >> ingest
    return dag


# 每天 17:00–19:45 每 15 分钟；周末文件日换算为空，接口直接返回
tiger_report_sync = tiger_report_sync_dag(
    "tiger_report_sync", '*/15 17-19 * * *', 'fileDate=' + TIGER_FILE_DATE, 1, timedelta(minutes=5))

# 晚间兜底两轮，与尊嘉 statement_sync_file_2010 / _23 同一思路
tiger_report_sync_late = tiger_report_sync_dag(
    "tiger_report_sync_late", '50 20,23 * * *', 'fileDate=' + TIGER_FILE_DATE, 3, timedelta(minutes=30))
