"""
Spotify Data Pipeline DAG
Scheduled to run populate_playlists_pipeline and run_audio_features_pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, '/opt/airflow/app')

from populatetables import populate_playlists_pipeline
from audiofeatures import run_audio_features_pipeline


default_args = {
    "owner": "spotify_tracker",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_data_pipeline",
    default_args=default_args,
    description="Daily pipeline to populate playlists and audio features",
    schedule_interval="0 2 * * *",  # 2 AM daily - adjust as needed
    start_date=datetime(2026, 1, 25),
    catchup=False,
    tags=["spotify", "etl"],
) as dag:

    populate_playlists_task = PythonOperator(
        task_id="populate_playlists",
        python_callable=populate_playlists_pipeline,
    )

    audio_features_task = PythonOperator(
        task_id="run_audio_features",
        python_callable=run_audio_features_pipeline,
    )

    # Run populate_playlists first, then audio_features
    populate_playlists_task >> audio_features_task
