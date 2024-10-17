import logging
import argparse
from typing import Optional, Sequence
from data_science_pipeline.utils.notebook import run_notebook


LOGGER = logging.getLogger(__name__)


def run(max_manuscripts: Optional[int] = None):
    LOGGER.info('Running pipeline for max_manuscripts: %s', max_manuscripts)
    run_notebook(
        notebook_filename='peerscout/peerscout-recommend-senior-editors.ipynb',
        notebook_params={'max_manuscripts': max_manuscripts}
    )
    run_notebook(
        notebook_filename=(
            'peerscout/peerscout-update-manuscript-version-matching-editor-profile.ipynb'
        ),
        notebook_params={'max_manuscripts': max_manuscripts}
    )
    LOGGER.info('ETL process completed successfully.')


def main(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-manuscripts', type=int, required=False)
    args = parser.parse_args(argv)
    run(max_manuscripts=args.max_manuscripts)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
