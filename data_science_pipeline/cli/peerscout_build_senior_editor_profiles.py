import logging
from data_science_pipeline.utils.notebook import run_notebook


LOGGER = logging.getLogger(__name__)


def main():
    run_notebook(
        notebook_filename='peerscout/peerscout-build-senior-editor-profiles.ipynb'
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
