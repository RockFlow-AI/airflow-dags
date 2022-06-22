import pendulum
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s

# 汇率更新
from dags.rockflow.operators.const import K8S_CONFIG_FILE

mysql_to_sensor = DAG(
    "mysql_to_sensor",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
    schedule_interval='30 0 * * 1-5',
    concurrency=1,
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

k = KubernetesPodOperator(
    name="mysql_to_sensor",
    namespace="data",
    image="rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/flow-data-connector:1.0.0",
    image_pull_secrets=[k8s.V1LocalObjectReference('registry-tmp')],
    labels={"app.kubernetes.io/name": "flow-data-connector"},
    task_id="mysql_to_sensor",
    do_xcom_push=True,
    volumes=[
        k8s.V1Volume(
            name='connector-pvc',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='flow-data-connector-pvc'),
        ),
        k8s.V1Volume(
            name='connector-config',
            config_map=k8s.V1ConfigMapEnvSource(name='flow-data-connector-config'),
        ),
    ],
    volume_mounts=[
        k8s.V1VolumeMount(name='connector-pvc', mount_path='/data/flow-data-connector', sub_path=None, read_only=False),
        k8s.V1VolumeMount(name='connector-config', mount_path="/config.yml", sub_path='config.yml', read_only=True),
    ],
    is_delete_operator_pod=True,
    config_file=K8S_CONFIG_FILE,
    get_logs=True,
    dag=mysql_to_sensor,
)

k.dry_run()
