import logging
import os
import re
from datetime import timedelta
from typing import Optional

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from data_science_pipeline.utils.notebook import run_notebook

LOGGER = logging.getLogger(__name__)

DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_SCHEDULE_INTERVAL"
)

DEFAULT_ARGS = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "provide_context": False,
}


# daily at 2am to allow other data being updated
DEFAULT_DATA_SCIENCE_SCHEDULE_INTERVAL = "0 2 * * *"


def get_default_dag_args() -> dict:
    return dict(
        schedule_interval=os.getenv(
            DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME,
            DEFAULT_DATA_SCIENCE_SCHEDULE_INTERVAL
        ),
        catchup=False,
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=60),
        max_active_runs=20,
        max_active_tasks=1
    )


def create_dag(
        dag_id: str,
        **kwargs) -> DAG:
    return DAG(
        dag_id=dag_id,
        **{
            **get_default_dag_args(),
            **kwargs
        }
    )


def get_default_notebook_task_id(notebook_filename: str) -> str:
    return (
        re.sub(
            r'[^a-z0-9]',
            '_',
            os.path.splitext(os.path.basename(notebook_filename))[0].lower()
        )
    )


def create_run_notebook_operator(
        notebook_filename: str,
        task_id: Optional[str] = None,
        notebook_params: Optional[dict] = None,
        **kwargs):
    if not task_id:
        task_id = get_default_notebook_task_id(notebook_filename)
    return PythonOperator(
        task_id=task_id,
        python_callable=run_notebook,
        op_kwargs={
            'notebook_filename': notebook_filename,
            'notebook_params': notebook_params or {}
        },
        **kwargs
    )
