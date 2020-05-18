import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_science_pipeline.utils.dags import (
    run_notebook
)


LOGGER = logging.getLogger(__name__)


DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_SCHEDULE_INTERVAL"
)

DAG_ID = "Sample_Data_Science_Data_Pipeline"
DEFAULT_ARGS = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "provide_context": False,
}

DATA_SCIENCE_DAG = DAG(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME,
        "@daily"
    ),
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=20,
    concurrency=30
)


NOTEBOOK_RUN_TASK = PythonOperator(
    task_id="Run_Jupyter_Notebook",
    dag=DATA_SCIENCE_DAG,
    python_callable=run_notebook,
    op_kwargs={
        'notebook_file_name': 'example.ipynb',
        'notebook_param': {}
    }
)
