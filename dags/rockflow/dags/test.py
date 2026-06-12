from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

fetch_news = KubernetesPodOperator(
    task_id="test______fetch_news",
    name="fetch-news",
    namespace="prod",
    image="rockflow-registry.ap-southeast-1.cr.aliyuncs.com/packages/content-platform-airflow:630dd2137aac0e2a42ca128faf6fb1265432895f",

    cmds=["python"],
    arguments=["-m", "jobs.news"],

    get_logs=True,
    is_delete_operator_pod=True,
)