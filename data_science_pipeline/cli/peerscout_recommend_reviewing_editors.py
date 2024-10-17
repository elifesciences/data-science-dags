import argparse
import logging
from typing import Optional, Sequence
from data_science_pipeline.utils.notebook import run_notebook


LOGGER = logging.getLogger(__name__)


def run(max_manuscripts: Optional[int] = None):
    run_notebook(
        notebook_filename='peerscout/peerscout-recommend-reviewing-editors.ipynb',
        notebook_params={'max_manuscripts': max_manuscripts}
    )
    run_notebook(
        notebook_filename='/'.join([
            'peerscout',
            'peerscout-update-manuscript-version-matching-reviewing-editor-profile.ipynb'
        ])
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-manuscripts', type=int, required=False)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    run(max_manuscripts=args.max_manuscripts)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
