import logging
from data_science_pipeline.cli.peerscout_recommend_senior_editors import main

LOGGER = logging.getLogger(__name__)


def test_peerscout_recommend_senior_editors():
    LOGGER.info("Running end2end test for peerscout_recommend_senior_editors")
    main(argv=['--max-manuscripts=1'])
