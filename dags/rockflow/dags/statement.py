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
    schedule_interval='50 7 * * 1-7',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
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

SimpleHttpOperator(
    task_id='statement_sync_ftp_file',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=statement_sync_ftp_file,
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
    endpoint='/inner/eod/us/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=sync_us_eod_file,
)

risk_debt_report = DAG(
    "risk_debt_report",
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
    task_id='risk_debt_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/risk/debt/send?date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=risk_debt_report,
)

US_trade_match_report = DAG(
    "US_trade_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='00 10 * * 1-7',
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
    endpoint='/inner/statement/tradeMatch/daily?market=US&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=US_trade_match_report,
)

HK_trade_match_report = DAG(
    "HK_trade_match_report",
    catchup=False,
    start_date=datetime(2023, 3, 2, 0, 0),
    schedule_interval='00 10 * * 1-7',
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
    endpoint='/inner/statement/tradeMatch/daily?market=HK&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=HK_trade_match_report,
)
