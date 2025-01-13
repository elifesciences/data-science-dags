from typing import Iterable
from unittest.mock import patch

import pytest


@pytest.fixture()
def mock_env() -> Iterable[dict]:
    env_dict: dict = {}
    with patch('os.environ', env_dict):
        yield env_dict
