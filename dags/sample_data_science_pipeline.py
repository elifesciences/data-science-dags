import os
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import papermill as pm

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

DATA_SCIENCE_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_SCHEDULE_INTERVAL"
)
AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME = (
    "AIRFLOW_APPLICATIONS_DIRECTORY_PATH"
)

APP_DIR_NAME_IN_AIRFLOW_APP_DIR = (
    "notebooks"
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


def run_notebook(
        notebook_file_name: str,
        notebook_param,
):
    notebook_path = str(Path(
        os.getenv(
            AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME,
            ""
        )
    ).joinpath(
        APP_DIR_NAME_IN_AIRFLOW_APP_DIR,
        notebook_file_name
    ))
    notebook_param = notebook_param or {}
    with TemporaryDirectory() as tmp_dir:
        temp_output_notebook_path = os.fspath(
            Path(tmp_dir, notebook_file_name)
        )
        pm.execute_notebook(
            notebook_path,
            temp_output_notebook_path,
            parameters=notebook_param,
            progress_bar=False,
            report_mode=True
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
