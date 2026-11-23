import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

qyzj_statement_sync_ftp_file = DAG(
    "qyzj_statement_sync_ftp_file",
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
    task_id='qyzj_statement_sync_ftp_file',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_ftp_file,
)

qyzj_statement_sync_delay_file = DAG(
    "qyzj_statement_sync_delay_file",
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
    task_id='qyzj_statement_sync_delay_file',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_delay_file,
)

# tradeDetail 兜底
qyzj_statement_sync_delay_file_1755 = DAG(
    "qyzj_statement_sync_delay_file_1755",
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
    task_id='qyzj_statement_sync_delay_file_1755',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_delay_file_1755,
)

# 同步文件20:10兜底
qyzj_statement_sync_file_2010 = DAG(
    "qyzj_statement_sync_file_2010",
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
    task_id='qyzj_statement_sync_file_2010',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_file_2010,
)

# 同步文件 T-1
qyzj_statement_sync_ftp_file_previous = DAG(
    "qyzj_statement_sync_ftp_file_previous",
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
    task_id='qyzj_statement_sync_ftp_file_previous',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=(datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_ftp_file_previous,
)

# 行权11点兜底
qyzj_statement_sync_file_23 = DAG(
    "qyzj_statement_sync_file_23",
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
    task_id='qyzj_statement_sync_file_23',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/inner/statement/ftpFiles/sync?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=qyzj_statement_sync_file_23,
)