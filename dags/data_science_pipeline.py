import os
import glob
from tempfile import TemporaryDirectory
from pathlib import Path
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

DAG_ID = "Data_Science_Data_Pipeline"
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


def run_notebook():
    app_files_location = os.getenv(
        AIRFLOW_APPLICATIONS_DIRECTORY_PATH_ENV_NAME,
        ""
    )
    app_files_location = os.fspath(
        Path(
            app_files_location,
            APP_DIR_NAME_IN_AIRFLOW_APP_DIR
        )
    )
    notebook_file_paths = glob.glob(app_files_location + "/*.ipynb")
    notebook_param = {}
    with TemporaryDirectory() as tmp_dir:
        for file_path in notebook_file_paths:
            file_name = os.path.basename(file_path)
            temp_output_notebook_path = os.fspath(
                Path(tmp_dir, file_name)
            )
            pm.execute_notebook(
                file_path,
                temp_output_notebook_path,
                parameters=notebook_param,
                progress_bar=False,
                report_mode=True
            )


NOTEBOOK_RUN_TASK = PythonOperator(
    task_id="Run_Jupyter_Notebook",
    dag=DATA_SCIENCE_DAG,
    python_callable=run_notebook,
)
