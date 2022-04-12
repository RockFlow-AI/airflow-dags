from datetime import datetime

from airflow.models import DAG

from dags.rockflow.operators.google_translate import GoogleTranslateOperator

with DAG(
        "app_review_translate",
        catchup=True,
        start_date=datetime.now(),
        schedule_interval="@once",
        default_args={
            "owner": "yinxiang",
            "depends_on_past": False,
            "retries": 0,
        }
) as app_review_translate:
    translate_reviews = GoogleTranslateOperator(
        files=['apple_review_df.json', 'google_review_df.json'],
    )
