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
    schedule_interval='30 7 * * 1-7',
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
    extra_options={"timeout": 600},
    dag=statement_sync_ftp_file,
)

option_exercise_report = DAG(
    "option_exercise_report",
    catchup=False,
    start_date=datetime(2022, 12, 5, 0, 0),
    schedule_interval='30 9 * * 1-7',
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
    endpoint='/inner/option/exercise/send',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_exercise_report,
)