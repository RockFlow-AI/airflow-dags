import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from rockflow.operators.const import CONTAINER_REPO, DW_CONNECTOR_VERSION, MQ_CONNECTOR_VERSION, DW_RAW_VERSION

with DAG(
        "mysql_to_sensor",
        catchup=False,
        start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
        schedule_interval='0 */1 * * *',
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
        task_id="mysql_to_sensor",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='connector-pvc',
                            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                                claim_name='sensordata-nfs'),
                        ),
                        k8s.V1Volume(
                            name='connector-config',
                            config_map=k8s.V1ConfigMapEnvSource(name='flow-data-connector-config'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='flow-data-connector',
                            image=f"{CONTAINER_REPO}/flow-data-connector:{DW_CONNECTOR_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='connector-pvc',
                                                  mount_path='/data',
                                                  sub_path=None,
                                                  read_only=False),
                                k8s.V1VolumeMount(name='connector-config',
                                                  mount_path="/config-sensor.yml",
                                                  sub_path='config-sensor.yml',
                                                  read_only=True),
                            ],
                        )
                    ],
                ),
            ),
        },
    )

with DAG(
        "mysql_to_kafka",
        catchup=False,
        start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
        schedule_interval='0 */1 * * *',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag_kafka:
    def callback():
        print("callback received")


    k_kafka = PythonOperator(
        task_id="mysql_to_kafka",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='connector-pvc',
                            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                                claim_name='sensordata-nfs'),
                        ),
                        k8s.V1Volume(
                            name='connector-admin',
                            config_map=k8s.V1ConfigMapEnvSource(name='flow-data-connector-admin'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='flow-data-connector-admin',
                            image=f"{CONTAINER_REPO}/flow-data-connector-admin:{MQ_CONNECTOR_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='connector-pvc',
                                                  mount_path='/data',
                                                  sub_path=None,
                                                  read_only=False),
                                k8s.V1VolumeMount(name='connector-admin',
                                                  mount_path="/config-admin.yml",
                                                  sub_path='config-admin.yml',
                                                  read_only=True),
                            ],
                        )
                    ],
                ),
            ),
        },
    )

with DAG(
        "dw_raw",
        catchup=False,
        start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
        schedule_interval='*/10 * * * *',
        concurrency=1,
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag_dw_raw:
    def callback():
        print("callback received")


    k_dw_raw = PythonOperator(
        task_id="dw_raw",
        python_callable=callback,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(
                            name='connector-pvc',
                            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                                claim_name='sensordata-nfs'),
                        ),
                        k8s.V1Volume(
                            name='connector-dw-raw',
                            config_map=k8s.V1ConfigMapEnvSource(name='flow-data-connector-dw-raw'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='flow-data-connector-dw-raw',
                            image=f"{CONTAINER_REPO}/flow-data-connector-dw-raw:{DW_RAW_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='connector-pvc',
                                                  mount_path='/data',
                                                  sub_path=None,
                                                  read_only=False),
                                k8s.V1VolumeMount(name='connector-dw-raw',
                                                  mount_path="/config-dw-raw.yml",
                                                  sub_path='config-dw-raw.yml',
                                                  read_only=True),
                            ],
                        )
                    ],
                ),
            ),
        },
    )
