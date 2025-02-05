import logging
from pathlib import Path
from typing import Iterable
from unittest.mock import patch

import pytest


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    for name in ('tests', 'data_science_pipeline'):
        logging.getLogger(name).setLevel('DEBUG')


@pytest.fixture
def temp_dir(tmp_path: Path):
    return tmp_path


@pytest.fixture()
def mock_env() -> Iterable[dict]:
    env_dict: dict = {}
    with patch('os.environ', env_dict):
        yield env_dict
