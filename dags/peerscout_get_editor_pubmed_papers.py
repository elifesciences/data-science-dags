# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os

from data_science_pipeline.utils.dags import (
    create_dag,
    create_run_notebook_operator
)


DEFAULT_PEERSCOUT_PEERSCOUT_EDITOR_PUBMED_SCHEDULE = '@hourly'


DATA_SCIENCE_PEERSCOUT_EDITOR_PUBMED_SCHEDULE_INTERVAL_ENV_NAME = (
    "DATA_SCIENCE_PEERSCOUT_EDITOR_PUBMED_SCHEDULE_INTERVAL"
)


DAG_ID = "Data_Science_PeerScout_Get_Editor_Pubmed_Papers"


def get_schedule_interval() -> str:
    return os.getenv(
        DATA_SCIENCE_PEERSCOUT_EDITOR_PUBMED_SCHEDULE_INTERVAL_ENV_NAME,
        DEFAULT_PEERSCOUT_PEERSCOUT_EDITOR_PUBMED_SCHEDULE
    )


# Note: need to save dag to a variable
with create_dag(
        dag_id=DAG_ID,
        schedule_interval=get_schedule_interval()) as dag:
    _ = (
        create_run_notebook_operator(
            notebook_filename='peerscout/peerscout-get-editor-parse-pubmed-links.ipynb'
        ) >> create_run_notebook_operator(
            notebook_filename='peerscout/peerscout-get-editor-pubmed-bibliography-paper-ids.ipynb'
        ) >> create_run_notebook_operator(
            notebook_filename='peerscout/peerscout-get-editor-pubmed-paper-ids.ipynb',
            # Note: The limit is more for development purpose
            notebook_params={
                'max_editors': 1000
            }
        ) >> create_run_notebook_operator(
            notebook_filename=(
                'peerscout/peerscout-get-editor-pubmed-external-manuscript-summary.ipynb'
            ),
            # Note: 100k end up around 85 MB of gzipped json
            #   we can spread the manuscripts over multiple runs
            notebook_params={
                'max_manuscripts': 1000000
            }
        ) >> create_run_notebook_operator(
            notebook_filename='peerscout/peerscout-disambiguate-editor-papers-details.ipynb'
        ) >> create_run_notebook_operator(
            notebook_filename='peerscout/peerscout-disambiguate-editor-papers.ipynb'
        )
    )
