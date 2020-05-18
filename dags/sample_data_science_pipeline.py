import logging

# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


LOGGER = logging.getLogger(__name__)


DAG_ID = "Sample_Data_Science_Data_Pipeline"


DATA_SCIENCE_DAG = create_dag(
    dag_id=DAG_ID
)
print('DATA_SCIENCE_DAG:', DATA_SCIENCE_DAG)

NOTEBOOK_RUN_TASK = create_run_notebook_operator(
    notebook_filename='example.ipynb',
    dag=DATA_SCIENCE_DAG,
)
