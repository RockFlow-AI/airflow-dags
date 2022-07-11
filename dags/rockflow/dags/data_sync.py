import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

with DAG(
        "mysql_to_sensor",
        catchup=False,
        start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
        schedule_interval='45 20 * * 1-5',
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
                                claim_name='flow-data-connector-pvc'),
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
                            image="rockflow-registry-vpc.ap-southeast-1.cr.aliyuncs.com/packages/flow-data-connector:1.1.0",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='connector-pvc',
                                                  mount_path='/data/flow-data-connector',
                                                  sub_path=None,
                                                  read_only=False),
                                k8s.V1VolumeMount(name='connector-config',
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
