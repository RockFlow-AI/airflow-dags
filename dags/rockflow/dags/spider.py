import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from rockflow.operators.const import CONTAINER_REPO, SPIDER_VERSION

with DAG(
        "spider",
        catchup=False,
        start_date=pendulum.datetime(2025, 7, 23, tz='America/New_York'),
        schedule_interval='0 9/4 * * *',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag:
    def callback():
        print("callback received")


    k = PythonOperator(
        task_id="spider",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='spider-config',
                            config_map=k8s.V1ConfigMapEnvSource(name='spider-config'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='spider',
                            image=f"{CONTAINER_REPO}/spider:{SPIDER_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='spider-config',
                                                  mount_path="/config.yml",
                                                  sub_path='config.yml',
                                                  read_only=True),
                            ],
                            security_context={
                                'privileged': True,
                                'allowPrivilegeEscalation': True,
                                'runAsUser': 0,
                                'capabilities': {'add': ['SYS_ADMIN']}
                            },
                        )
                    ],
                ),
            ),
        },
    )

with DAG(
        "spider-clearing",
        catchup=False,
        start_date=pendulum.datetime(2025, 7, 23, tz='America/New_York'),
        schedule_interval='0 2 * * *',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag:
    def callback():
        print("callback received")


    k = PythonOperator(
        task_id="spider-clearing",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='spider-clearing-config',
                            config_map=k8s.V1ConfigMapEnvSource(name='spider-clearing-config'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='spider-clearing',
                            image=f"{CONTAINER_REPO}/spider:{SPIDER_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='spider-clearing-config',
                                                  mount_path="/config.yml",
                                                  sub_path='config-clearing.yml',
                                                  read_only=True),
                            ],
                            security_context={
                                'privileged': True,
                                'allowPrivilegeEscalation': True,
                                'runAsUser': 0,
                                'capabilities': {'add': ['SYS_ADMIN']}
                            },
                        )
                    ],
                ),
            ),
        },
    )
