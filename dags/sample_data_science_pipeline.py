# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


DAG_ID = "Sample_Data_Science_Data_Pipeline"


# Note: need to save dag to a variable
with create_dag(dag_id=DAG_ID) as dag:
    create_run_notebook_operator(
        notebook_filename='example.ipynb',
    )
