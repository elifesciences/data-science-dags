from pathlib import Path

import pytest
from py._path.local import LocalPath


@pytest.fixture
def temp_dir(tmpdir: LocalPath):
    # convert to standard Path
    return Path(str(tmpdir))
