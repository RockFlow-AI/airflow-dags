import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

nz_statement_sync_ftp_file = DAG(
    "nz_statement_sync_ftp_file",
    catchup=False,
    start_date=datetime(2022, 10, 22, 0, 0),
    schedule_interval='50 7,2,1 * * 1-7',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=30)
    }
)

SimpleHttpOperator(
    task_id='nz_statement_sync_ftp_file',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_ftp_file,
)

nz_statement_sync_delay_file = DAG(
    "nz_statement_sync_delay_file",
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
    task_id='nz_statement_sync_delay_file',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_delay_file,
)

# tradeDetail 兜底
nz_statement_sync_delay_file_1755 = DAG(
    "nz_statement_sync_delay_file_1755",
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
    task_id='nz_statement_sync_delay_file_1755',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_delay_file_1755,
)

# 同步文件20:10兜底
nz_statement_sync_file_2010 = DAG(
    "nz_statement_sync_file_2010",
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
    task_id='nz_statement_sync_file_2010',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_file_2010,
)

# 同步文件 T-1
nz_statement_sync_ftp_file_previous = DAG(
    "nz_statement_sync_ftp_file_previous",
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
    task_id='nz_statement_sync_ftp_file_previous',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_ftp_file_previous,
)

# 行权11点兜底
nz_statement_sync_file_23 = DAG(
    "nz_statement_sync_file_23",
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
    task_id='nz_statement_sync_file_23',
    method='PATCH',
    http_conn_id='flow-statement-nz',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=nz_statement_sync_file_23,
)

NZ_US_trade_match_report = DAG(
    "NZ_US_trade_match_report",
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
    task_id='NZ_US_trade_match_report',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statements/tradeMatch/daily?market=US&date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=NZ_US_trade_match_report,
)