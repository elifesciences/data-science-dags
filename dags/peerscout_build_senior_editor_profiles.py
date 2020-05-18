import logging
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import papermill as pm


LOGGER = logging.getLogger(__name__)


DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_SCHEDULE_INTERVAL"
)
AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME = (
    "AIRFLOW_APPLICATIONS_DIRECTORY_PATH"
)

DATA_SCIENCE_SOURCE_DATASET_ENV_NAME = (
    "DATA_SCIENCE_SOURCE_DATASET"
)

DATA_SCIENCE_OUTPUT_DATASET_ENV_NAME = (
    "DATA_SCIENCE_OUTPUT_DATASET"
)

DEFAULT_OUTPUT_TABLE_PREFIX = 'data_science_'

APP_DIR_NAME_IN_AIRFLOW_APP_DIR = (
    "notebooks"
)

DAG_ID = "PeerScout_Build_Senior_Editor_Profiles"
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


def get_default_notebook_params() -> dict:
    deployment_env = get_deployment_env()
    return {
        'deployment_env': deployment_env,
        'source_dataset': get_default_source_dataset(deployment_env),
        'output_dataset': get_default_output_dataset(deployment_env),
        'output_table_prefix': DEFAULT_OUTPUT_TABLE_PREFIX
    }


def get_combined_notebook_params(
        default_notebook_params: dict,
        override_notebook_param: dict = None) -> dict:
    return {
        **default_notebook_params,
        **(override_notebook_param or {})
    }


def get_notebook_path(notebook_file_name: str) -> str:
    return str(Path(
        os.getenv(
            AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME,
            ""
        )
    ).joinpath(
        APP_DIR_NAME_IN_AIRFLOW_APP_DIR,
        notebook_file_name
    ))


def run_notebook(
        notebook_file_name: str,
        notebook_param: dict = None,
):
    notebook_path = get_notebook_path(notebook_file_name)
    notebook_param = get_combined_notebook_params(
        get_default_notebook_params(),
        notebook_param
    )
    LOGGER.info('processing %r with parameters: %s', notebook_path, notebook_param)
    with TemporaryDirectory() as tmp_dir:
        temp_output_notebook_path = os.fspath(
            Path(tmp_dir, os.path.basename(notebook_file_name))
        )
        pm.execute_notebook(
            notebook_path,
            temp_output_notebook_path,
            parameters=notebook_param,
            progress_bar=False,
            log_output=True,
            stdout_file=sys.stdout,
            stderr_file=sys.stderr,
            report_mode=True
        )


NOTEBOOK_RUN_TASK = PythonOperator(
    task_id="Run_Jupyter_Notebook",
    dag=DATA_SCIENCE_DAG,
    python_callable=run_notebook,
    op_kwargs={
        'notebook_file_name': 'peerscout/peerscout-build-senior-editor-profiles.ipynb',
        'notebook_param': {}
    }
)
