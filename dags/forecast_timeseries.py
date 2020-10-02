# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


DAG_ID = "Data_Science_Forecast_Timeseries"


DEFAULT_FORECAST_SCHEDULE = '@hourly'

DATA_SCIENCE_FORECAST_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_FORECAST_SCHEDULE_INTERVAL"
)


def get_schedule_interval() -> str:
    return os.getenv(
        DATA_SCIENCE_FORECAST_SCHEDULE_INTERVAL_ENV_NAME,
        DEFAULT_FORECAST_SCHEDULE
    )


# Note: need to save dag to a variable
with create_dag(dag_id=DAG_ID, schedule_interval=get_schedule_interval()) as dag:
    create_run_notebook_operator(
        task_id='Forecast_Initial_Submission',
        notebook_filename='forecasting/forecast-timeseries.ipynb',
        notebook_params={
            'sql_filename': 'initial_submission_count_by_date.sql',
            'output_table_name': 'Forecast_Initial_Submission'
        }
    )
    create_run_notebook_operator(
        task_id='Forecast_Full_Submission_Received_To_First_Decision',
        notebook_filename='forecasting/forecast-timeseries.ipynb',
        notebook_params={
            'sql_filename': 'full_submission_received_to_first_decision_by_date.sql',
            'output_table_name': 'Forecast_Full_Submission_Received_To_First_Decision'
        }
    )
