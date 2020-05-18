import logging

# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


LOGGER = logging.getLogger(__name__)


DAG_ID = "PeerScout_Build_Senior_Editor_Profiles"


# Note: need to save dag to a variable
with create_dag(dag_id=DAG_ID) as dag:
    create_run_notebook_operator(
        notebook_filename='peerscout/peerscout-build-senior-editor-profiles.ipynb'
    )
