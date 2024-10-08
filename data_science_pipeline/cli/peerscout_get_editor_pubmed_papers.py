import logging
from data_science_pipeline.utils.notebook import run_notebook

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def main():
    run_notebook(
        notebook_filename='peerscout/peerscout-get-editor-parse-pubmed-links.ipynb'
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-get-editor-pubmed-bibliography-paper-ids.ipynb'
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-get-editor-pubmed-paper-ids.ipynb',
        # Note: The limit is more for development purpose
        notebook_params={'max_editors': 1000}
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-create-view-editor-pubmed-ids.ipynb'
    )
    run_notebook(
        notebook_filename=(
            'peerscout/peerscout-get-editor-pubmed-external-manuscript-summary.ipynb'
        ),
        # Note: 100k end up around 85 MB of gzipped json
        #   we can spread the manuscripts over multiple runs
        notebook_params={'max_manuscripts': 1000000}
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-disambiguate-editor-papers-details.ipynb'
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-disambiguate-editor-papers.ipynb'
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
