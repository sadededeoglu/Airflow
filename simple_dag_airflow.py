import pendulum
import warnings
import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from pandas.errors import SettingWithCopyWarning
from airflow.operators.email import EmailOperator
from utils.callbacks import send_mail_on_dag_failure
from airflow.operators.dummy_operator import DummyOperator
from basic_cron_trigger_timetable_plugin import BasicCronTriggerTimetable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# Suppress unnecessary warnings for cleaner logs
warnings.filterwarnings("ignore", category=UserWarning)
warnings.simplefilter(action='ignore', category=(SettingWithCopyWarning))

# Define local timezone for scheduling
local_tz = pendulum.timezone("Europe/Istanbul")  # Sets the timezone to Istanbul

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'Specify DAG owner',  # Person or team responsible for the DAG
    'email': [Variable.get("mail_airflow")],  # Email list to send notifications
    'retries': 3,  # Number of retry attempts before marking a task as failed
    'email_on_failure': False,  # Disable automatic email on task failure
    'email_on_retry': False  # Disable email notifications for retries
}

with DAG(
    dag_id='DAG_ID_TO_DISPLAY_IN_AIRFLOW',  # Unique DAG ID shown in Airflow UI
    default_args=default_args,
    description='Descriptive text explaining the purpose of this DAG',
    # Use BasicCronTriggerTimetable for precise scheduling; incorrect format may cause issues in DR runs
    schedule_interval=BasicCronTriggerTimetable('30 23 * * *', timezone=local_tz),
    start_date=datetime(2024, 12, 30, tzinfo=pendulum.timezone("Europe/Istanbul")),  # Start date for the DAG
    dagrun_timeout=timedelta(minutes=60),  # Maximum runtime before DAG run is marked as failed
    on_failure_callback=send_mail_on_dag_failure,  # Function to send a custom email on DAG failure
    catchup=False,  # Prevent backfilling of missed runs
    tags=["ACTIVE", "store", "daily", "endofday", "mail"]  # Keywords for filtering/searching DAGs in Airflow UI
) as dag:
    pass
