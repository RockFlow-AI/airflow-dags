import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

copy_trading_copy_position_us = DAG(
    "copy_trading_copy_position_us",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='35 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_copy_position_us',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/init?market=US',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_copy_position_us,
)

copy_trading_copy_position_hk = DAG(
    "copy_trading_copy_position_hk",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='Asia/Shanghai'),
    schedule_interval='35 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_copy_position_hk',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/init?market=HK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_copy_position_hk,
)


copy_trading_copy_position_hk_pm = DAG(
    "copy_trading_copy_position_hk_pm",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='Asia/Shanghai'),
    schedule_interval='05 13 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_copy_position_hk_pm',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/init?market=HK',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_copy_position_hk_pm,
)

copy_trading_auto_correction_us = DAG(
    "copy_trading_auto_correction_us",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='40 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction_us',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}&market=US&type=1'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction_us,
)


copy_trading_auto_correction_failure_order_us = DAG(
    "copy_trading_auto_correction_failure_order_us",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='0 11-16 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction_failure_order_us',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}&market=US&type=2'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction_failure_order_us,
)


copy_trading_auto_correction_different_positions_us = DAG(
    "copy_trading_auto_correction_different_positions_us",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='America/New_York'),
    schedule_interval='30 15 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction_different_positions_us',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}&market=US&type=3'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction_different_positions_us,
)


copy_trading_auto_correction_hk = DAG(
    "copy_trading_auto_correction_hk",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='Asia/Shanghai'),
    schedule_interval='40 9 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction_hk',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}&market=HK&type=1'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction_hk,
)


copy_trading_auto_correction_failure_order_hk = DAG(
    "copy_trading_auto_correction_failure_order_hk",
    catchup=False,
    start_date=pendulum.datetime(2024, 10, 14, tz='Asia/Shanghai'),
    schedule_interval='0 11-16 * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_auto_correction_failure_order_hk',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/position/correct?tradeDay={date}&market=HK&type=2'.format(date=datetime.now().strftime("%Y-%m-%d")),
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_auto_correction_failure_order_hk,
)


copy_trading_commission_period_refresh = DAG(
    "copy_trading_commission_period_refresh",
    catchup=False,
    start_date=pendulum.datetime(2026, 3, 14, tz='Asia/Shanghai'),
    schedule_interval='0 0 1 1,4,7,10 *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='copy_trading_commission_period_refresh',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/commission/periodRecords/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_trading_commission_period_refresh,
)