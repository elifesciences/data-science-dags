import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
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
        override_notebook_params: Optional[dict] = None) -> dict:
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
        notebook_params: Optional[dict] = None,
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
