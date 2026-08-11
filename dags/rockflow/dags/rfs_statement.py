import json
import pendulum
from airflow.models import DAG
from datetime import date, datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
from zoneinfo import ZoneInfo

rfs_statement = DAG(
    "rfs_statement",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 10, tz='Asia/Shanghai'),
    schedule_interval='00 21 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='rfs_statement',
    method='POST',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/rfs/export?statementDay={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rfs_statement,
)

import_statement = DAG(
    "import_statement",
    catchup=False,
    start_date=pendulum.datetime(2026, 4, 10, tz='Asia/Shanghai'),
    schedule_interval='00 20 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    }
)

import_statement_task = SimpleHttpOperator(
    task_id='import_statement',
    method='PATCH',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/ftpFiles/import?date={date}'.format(date=datetime.now().strftime("%Y%m%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

verify_statement_imported = SimpleHttpOperator(
    task_id='verify_statement_imported',
    method='GET',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/zv/count?statementDate={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200 and response.json()['data']['balanceCount'] >= 14,
    extra_options={"timeout": 60},
    dag=import_statement,
)

import_data_to_statement_qyzj = SimpleHttpOperator(
    task_id='import_data_to_statement_qyzj',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint='/statement/inner/data/import',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)


replay_deposit_withdrawal_qyzj = SimpleHttpOperator(
    task_id='replay_deposit_withdrawal_qyzj',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint="/inner/statement/zv/handle/cash/inout/{date}".format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

replay_cash_change_qyzj = SimpleHttpOperator(
    task_id='replay_cash_change_qyzj',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint="/inner/statement/zv/handle/cash/changes/{date}".format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

replay_trades_qyzj = SimpleHttpOperator(
    task_id='replay_trades_qyzj',
    method='POST',
    headers={"Content-Type": "application/json"},
    data=json.dumps({"startTime":f"{(date.today() - timedelta(days=1)).strftime('%Y-%m-%d')}T04:00:00-04:00","endTime":f"{datetime.now().strftime('%Y-%m-%d')}T04:00:00-04:00"}),
    http_conn_id='flow-order-gateway-zv-usd.qyzj',
    endpoint='localhost:8081/orders/inner/send',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

replay_corporate_action_qyzj = SimpleHttpOperator(
    task_id='replay_corporate_action_qyzj',
    method='POST',
    http_conn_id='flow-ledger.qyzj',
    endpoint="/ledger/inner/corporateActions/confirm/{date}".format(date=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)


replay_option_exercise_qyzj = SimpleHttpOperator(
    task_id='replay_option_exercise_qyzj',
    method='PATCH',
    http_conn_id='flow-statement.qyzj',
    endpoint="/inner/option/exercise/send?date={date}".format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

ledger_reconcile_qyzj = SimpleHttpOperator(
    task_id='ledger_reconcile_qyzj',
    method='POST',
    http_conn_id='flow-statement.qyzj',
    endpoint="/inner/statement/zv/reconcile/reconcile?date={date}".format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

align_datetime_qyzj = SimpleHttpOperator(
    task_id='align_datetime_qyzj',
    method='PUT',
    http_conn_id='flow-statement.qyzj',
    endpoint=f'/statement/inner/data/datetime?fromDateTime={int((datetime.now(ZoneInfo("America/New_York")).replace(hour=4, minute=0, second=0, microsecond=0) - timedelta(days=1)).timestamp() * 1000)}',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=import_statement,
)

data_reconcile_qyzj = SimpleHttpOperator(
    task_id='data_reconcile_qyzj',
    method='GET',
    http_conn_id='flow-statement.qyzj',
    endpoint='/statement/inner/data/reconcile',
    response_check=lambda response: response.json()['code'] == 200 and len(response.json()['data']['misalignedCashRecords']) == 0 and len(response.json()['data']['misalignedPositionRecords']) == 0,
    extra_options={"timeout": 60},
    dag=import_statement,
)

import_statement_task >> verify_statement_imported >> import_data_to_statement_qyzj >> replay_deposit_withdrawal_qyzj >> replay_cash_change_qyzj >> replay_trades_qyzj >> replay_corporate_action_qyzj >> replay_option_exercise_qyzj >> ledger_reconcile_qyzj >> align_datetime_qyzj >> data_reconcile_qyzj
