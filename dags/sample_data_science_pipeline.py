import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG

from data_science_pipeline.utils.dags import (
    create_run_notebook_operator
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


NOTEBOOK_RUN_TASK = create_run_notebook_operator(
    notebook_filename='example.ipynb',
    dag=DATA_SCIENCE_DAG,
)
