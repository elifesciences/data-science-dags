import logging
from data_science_pipeline.utils.notebook import run_notebook

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def main():
    run_notebook(
        notebook_filename='peerscout/peerscout-recommend-reviewing-editors.ipynb'
    )
    run_notebook(
        notebook_filename='/'.join([
            'peerscout',
            'peerscout-update-manuscript-version-matching-reviewing-editor-profile.ipynb'
        ])
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
