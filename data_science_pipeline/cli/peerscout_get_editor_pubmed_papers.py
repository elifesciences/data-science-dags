import argparse
import logging
from typing import Optional, Sequence
from data_science_pipeline.utils.notebook import run_notebook


LOGGER = logging.getLogger(__name__)


# Note: 100k end up around 85 MB of gzipped json
#   we can spread the manuscripts over multiple runs
DEFAULT_MAX_MANUSCRIPTS = 1000000


def run(max_manuscripts: Optional[int] = None):
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
        notebook_params={'max_manuscripts': max_manuscripts}
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-disambiguate-editor-papers-details.ipynb'
    )
    run_notebook(
        notebook_filename='peerscout/peerscout-disambiguate-editor-papers.ipynb'
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--max-manuscripts',
        default=DEFAULT_MAX_MANUSCRIPTS,
        type=int,
        required=False
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    run(max_manuscripts=args.max_manuscripts)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
