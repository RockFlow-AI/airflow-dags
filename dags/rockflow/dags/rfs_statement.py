import logging
import json
from time import sleep

import pendulum
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.sdk import Variable
from datetime import date, datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
import requests
from urllib.parse import quote
from zoneinfo import ZoneInfo
from rockflow.operators.const import LARK_ALERT_USER_ID, AIRFLOW_API_BASE, MAX_DAG_RETRIES, DAG_RETRY_DELAY_SECONDS

logger = logging.getLogger("airflow.task")

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
        "provide_context": True,
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

def _attempts_key(dag_id: str, run_id: str) -> str:
    return f"dag_run_attempts::{dag_id}::{run_id}"


def _read_attempts(dag_id: str, run_id: str) -> int:
    return int(Variable.get(_attempts_key(dag_id, run_id), default=0))


def _bump_attempts(dag_id: str, run_id: str) -> int:
    new_value = _read_attempts(dag_id, run_id) + 1
    Variable.set(_attempts_key(dag_id, run_id), str(new_value))
    return new_value

def _cleanup_attempts(context):
    dag_run = context["dag_run"]
    Variable.delete(_attempts_key(dag_run.dag_id, dag_run.run_id))

def clear_dag_run_on_failure(context):
    dag_run = context["dag_run"]
    dag_id_path = quote(dag_run.dag_id, safe="")
    run_id_path = quote(dag_run.run_id, safe="")
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id_path}/dagRuns/{run_id_path}/clear"

    attempts = _read_attempts(dag_id_path, run_id_path)
    if attempts >= MAX_DAG_RETRIES:
        logger.info(f"Skipping due to max DAG retry reached on failure at {url}")
        _cleanup_attempts(context)
        return

    logger.info(f"Sleeping {DAG_RETRY_DELAY_SECONDS}s before clearing DAG run on failure at {url}")
    sleep(DAG_RETRY_DELAY_SECONDS)

    _bump_attempts(dag_id_path, run_id_path)
    logger.info(f"Clearing DAG run on failure {attempts}/{MAX_DAG_RETRIES} at {url}")
    response = requests.post(
        url,
        json={"dry_run": False},
        timeout=30,
    )
    response.raise_for_status()

verify_statement_imported = SimpleHttpOperator(
    task_id='verify_statement_imported',
    method='GET',
    http_conn_id='flow-statement',
    endpoint='/inner/statement/zv/count?statementDate={date}'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200 and response.json()['data']['balanceCount'] >= 14,
    on_failure_callback=clear_dag_run_on_failure,
    on_success_callback=_cleanup_attempts,
    extra_options={"timeout": 60},
    dag=import_statement,
)


def build_replay_tasks(dag):
    import_data_to_statement_qyzj = SimpleHttpOperator(
        task_id='import_data_to_statement_qyzj',
        method='PATCH',
        http_conn_id='flow-statement.qyzj',
        endpoint='/statement/inner/data/import',
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    replay_deposit_withdrawal_qyzj = SimpleHttpOperator(
        task_id='replay_deposit_withdrawal_qyzj',
        method='PATCH',
        http_conn_id='flow-statement.qyzj',
        endpoint="/inner/statement/zv/handle/cash/inout/{date}".format(date=datetime.now().strftime("%Y-%m-%d")),
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    replay_cash_change_qyzj = SimpleHttpOperator(
        task_id='replay_cash_change_qyzj',
        method='PATCH',
        http_conn_id='flow-statement.qyzj',
        endpoint="/inner/statement/zv/handle/cash/changes/{date}".format(date=datetime.now().strftime("%Y-%m-%d")),
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    replay_trades_qyzj = SimpleHttpOperator(
        task_id='replay_trades_qyzj',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps({"startTime":f"{(date.today() - timedelta(days=1)).strftime('%Y-%m-%d')}T04:00:00-04:00","endTime":f"{datetime.now().strftime('%Y-%m-%d')}T04:00:00-04:00"}),
        http_conn_id='flow-order-gateway-zv-usd.qyzj',
        endpoint='/orders/inner/send',
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    replay_corporate_action_qyzj = SimpleHttpOperator(
        task_id='replay_corporate_action_qyzj',
        method='POST',
        http_conn_id='flow-ledger.qyzj',
        endpoint="/ledger/inner/corporateActions/confirm/{date}".format(date=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d")),
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    replay_option_exercise_qyzj = SimpleHttpOperator(
        task_id='replay_option_exercise_qyzj',
        method='PATCH',
        http_conn_id='flow-statement.qyzj',
        endpoint="/inner/option/exercise/send?date={date}".format(date=datetime.now().strftime("%Y-%m-%d")),
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    ledger_reconcile_qyzj = SimpleHttpOperator(
        task_id='ledger_reconcile_qyzj',
        method='POST',
        http_conn_id='flow-statement.qyzj',
        endpoint="/inner/statement/zv/reconcile/reconcile?date={date}".format(date=datetime.now().strftime("%Y-%m-%d")),
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    align_datetime_qyzj = SimpleHttpOperator(
        task_id='align_datetime_qyzj',
        method='PUT',
        http_conn_id='flow-statement.qyzj',
        endpoint=f'/statement/inner/data/datetime?fromDateTime={int((datetime.now(ZoneInfo("America/New_York")).replace(hour=4, minute=0, second=0, microsecond=0) - timedelta(days=1)).timestamp() * 1000)}',
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    data_reconcile_qyzj = SimpleHttpOperator(
        task_id='data_reconcile_qyzj',
        method='GET',
        http_conn_id='flow-statement.qyzj',
        endpoint='/statement/inner/data/reconcile',
        response_check=lambda response: response.json()['code'] == 200,
        response_filter=lambda response: response.json(),
        extra_options={"timeout": 60},
        dag=dag,
    )

    cash_count_placeholder = "__CASH_COUNT__"
    position_count_placeholder = "__POSITION_COUNT__"
    lark_notification_data = json.dumps([{
        "userId": LARK_ALERT_USER_ID,
        "type": 4,
        "language": "zh-cn",
        "payload": {
            "userCashRecords": cash_count_placeholder,
            "userPositionRecords": position_count_placeholder,
        }}]).replace(
        f'"{cash_count_placeholder}"',
        "{{ task_instance.xcom_pull(task_ids='data_reconcile_qyzj')['data']['misalignedCashRecords'] | length }}"
    ).replace(
        f'"{position_count_placeholder}"',
        "{{ task_instance.xcom_pull(task_ids='data_reconcile_qyzj')['data']['misalignedPositionRecords'] | length }}"
    )

    lark_notification = SimpleHttpOperator(
        task_id='lark_notification',
        method='POST',
        http_conn_id='flow-notification',
        headers={"Content-Type": "application/json"},
        endpoint='/notification/inner/specification/notifications/specify/push/HK_DATA_RECONCILIATION',
        data=lark_notification_data,
        response_check=lambda response: response.json()['code'] == 200,
        extra_options={"timeout": 60},
        dag=dag,
    )

    import_data_to_statement_qyzj >> replay_deposit_withdrawal_qyzj >> replay_cash_change_qyzj >> replay_trades_qyzj >> replay_corporate_action_qyzj >> replay_option_exercise_qyzj >> ledger_reconcile_qyzj >> align_datetime_qyzj >> data_reconcile_qyzj >> lark_notification

    return import_data_to_statement_qyzj


replay_tasks_start = build_replay_tasks(import_statement)
import_statement_task >> verify_statement_imported >> replay_tasks_start

import_statement_saturday = DAG(
    "import_statement_saturday",
    catchup=False,
    start_date=pendulum.datetime(2026, 8, 12, tz='Asia/Shanghai'),
    schedule_interval='00 20 * * 6',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "provide_context": True,
    }
)

build_replay_tasks(import_statement_saturday)
