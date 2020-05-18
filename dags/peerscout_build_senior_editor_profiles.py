import logging

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


LOGGER = logging.getLogger(__name__)


DAG_ID = "PeerScout_Build_Senior_Editor_Profiles"


DATA_SCIENCE_DAG = create_dag(
    dag_id=DAG_ID
)

NOTEBOOK_RUN_TASK = create_run_notebook_operator(
    notebook_filename='peerscout/peerscout-build-senior-editor-profiles.ipynb',
    dag=DATA_SCIENCE_DAG
)
