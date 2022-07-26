import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from rockflow.operators.const import CONTAINER_REPO, OPTION_ALGO_VERSION, OPTION_ALGO_SHARDS


def option_algorithm_task(dag, shard: int) -> PythonOperator:
    def callback():
        print("callback received")

    return PythonOperator(
        dag=dag,
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
                            env=[
                                k8s.V1EnvVar(name="OPTION_SHARD", value=shard),
                                k8s.V1EnvVar(name="OPTION_SHARDS", value=OPTION_ALGO_SHARDS),
                            ],
                        )
                    ],
                ),
            ),
        },
    )


def option_dag_shard(shard: int):
    with DAG(
            f"option_algorithm_us_09-30_09-50_{shard}",
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
        option_algorithm_task(dag_us_1, shard)
        globals()[f'option_dag_us_1_{shard}'] = dag_us_1

    with DAG(
            f"option_algorithm_us_10-00_19-50_{shard}",
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
        option_algorithm_task(dag_us_2, shard)
        globals()[f'option_dag_us_2_{shard}'] = dag_us_2

    with DAG(
            f"option_algorithm_us_20-00_{shard}",
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
        option_algorithm_task(dag_us_3, shard)
        globals()[f'option_dag_us_3_{shard}'] = dag_us_3


for i in range(OPTION_ALGO_SHARDS):
    option_dag_shard(i)
