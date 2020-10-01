import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import papermill as pm


LOGGER = logging.getLogger(__name__)


DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME = (
    "AIRFLOW_APPLICATIONS_DIRECTORY_PATH"
)

APP_DIR_NAME_IN_AIRFLOW_APP_DIR = (
    "notebooks"
)


DATA_SCIENCE_SOURCE_DATASET_ENV_NAME = (
    "DATA_SCIENCE_SOURCE_DATASET"
)

DATA_SCIENCE_OUTPUT_DATASET_ENV_NAME = (
    "DATA_SCIENCE_OUTPUT_DATASET"
)

DEFAULT_OUTPUT_TABLE_PREFIX = 'data_science_'


DATA_SCIENCE_STATE_PATH_ENV_NAME = (
    "DATA_SCIENCE_STATE_PATH"
)
DEFAULT_STATE_PATH_FORMAT = (
    "s3://{env}-elife-data-pipeline/airflow-config/data-science/state"
)

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
        concurrency=1
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


def get_deployment_env() -> str:
    return os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV_VALUE
    )


def get_default_source_dataset(deployment_env: str) -> str:
    return os.getenv(
        DATA_SCIENCE_SOURCE_DATASET_ENV_NAME,
        deployment_env
    )


def get_default_output_dataset(deployment_env: str) -> str:
    return os.getenv(
        DATA_SCIENCE_OUTPUT_DATASET_ENV_NAME,
        deployment_env
    )


def get_default_state_path(deployment_env: str) -> str:
    return os.getenv(
        DATA_SCIENCE_STATE_PATH_ENV_NAME,
        DEFAULT_STATE_PATH_FORMAT.format(env=deployment_env)
    )


def get_default_notebook_params() -> dict:
    deployment_env = get_deployment_env()
    return {
        'deployment_env': deployment_env,
        'source_dataset': get_default_source_dataset(deployment_env),
        'output_dataset': get_default_output_dataset(deployment_env),
        'output_table_prefix': DEFAULT_OUTPUT_TABLE_PREFIX,
        'state_path': get_default_state_path(deployment_env)
    }


def get_combined_notebook_params(
        default_notebook_params: dict,
        override_notebook_params: dict = None) -> dict:
    return {
        **default_notebook_params,
        **(override_notebook_params or {})
    }


def get_notebook_path(notebook_filename: str) -> str:
    return str(Path(
        os.getenv(
            AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME,
            ""
        )
    ).joinpath(
        APP_DIR_NAME_IN_AIRFLOW_APP_DIR,
        notebook_filename
    ))


def run_notebook(
        notebook_filename: str,
        notebook_params: dict = None,
):
    notebook_path = get_notebook_path(notebook_filename)
    notebook_params = get_combined_notebook_params(
        get_default_notebook_params(),
        notebook_params
    )
    LOGGER.info('processing %r with parameters: %s', notebook_path, notebook_params)
    with TemporaryDirectory() as tmp_dir:
        temp_output_notebook_path = os.fspath(
            Path(tmp_dir, os.path.basename(notebook_filename))
        )
        pm.execute_notebook(
            notebook_path,
            temp_output_notebook_path,
            parameters=notebook_params,
            progress_bar=False,
            log_output=True,
            report_mode=True
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
        task_id: str = None,
        notebook_params: dict = None,
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
