import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from rockflow.operators.const import CONTAINER_REPO, DW_USERRISK_VERSION

with DAG(
        "dim_user_risk_sync",
        catchup=False,
        start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
        schedule_interval='0 0 * * *',
        concurrency=1,
        default_args={
            "owner": "xuwenzhe",
            "depends_on_past": False,
            "retries": 0,
        }
) as dag:
    def callback():
        print("callback received")


    k = PythonOperator(
        task_id="dim_user_risk_sync",
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
                            config_map=k8s.V1ConfigMapEnvSource(name='connector-dw-raw-userrisk'),
                        ),
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                        k8s.V1Container(
                            name='flow-data-connector-dw-userrisk',
                            image=f"{CONTAINER_REPO}/flow-data-connector:{DW_USERRISK_VERSION}",
                            volume_mounts=[
                                k8s.V1VolumeMount(name='connector-pvc',
                                                  mount_path='/data',
                                                  sub_path=None,
                                                  read_only=False),
                                k8s.V1VolumeMount(name='connector-config',
                                                  mount_path="/config-user-risk.yml",
                                                  sub_path='config-user-risk.yml',
                                                  read_only=True),
                            ],
                        )
                    ],
                ),
            ),
        },
    )
