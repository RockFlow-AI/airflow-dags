import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from rockflow.operators.const import CONTAINER_REPO, OPTION_ALGO_VERSION


def option_algorithm_task() -> PythonOperator:
    def callback():
        print("callback received")

    return PythonOperator(
        task_id="option_algorithm_us",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='option-algorithm-config',
                            config_map=k8s.V1ConfigMapEnvSource(name='flow-option-algorithm-config'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='flow-option-algorithm',
                            image=f"{CONTAINER_REPO}/flow-option-algorithm:{OPTION_ALGO_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='option-algorithm-config',
                                                  mount_path="/config.yml",
                                                  sub_path='config.yml',
                                                  read_only=True),
                            ],
                        )
                    ],
                ),
            ),
        },
    )


with DAG(
        "option_algorithm_us_09:30_09:50",
        catchup=False,
        start_date=pendulum.datetime(2022, 7, 22, tz='America/New_York'),
        schedule_interval='30-50/10 09 * * 1-5',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag_us_1:
    task_us_1 = option_algorithm_task()

with DAG(
        "option_algorithm_us_10:00_19:50",
        catchup=False,
        start_date=pendulum.datetime(2022, 7, 22, tz='America/New_York'),
        schedule_interval='*/10 10-19 * * 1-5',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag_us_2:
    task_us_2 = option_algorithm_task()

with DAG(
        "option_algorithm_us_20:00",
        catchup=False,
        start_date=pendulum.datetime(2022, 7, 22, tz='America/New_York'),
        schedule_interval='0 20 * * 1-5',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag_us_3:
    task_us_3 = option_algorithm_task()
