import os
from unittest.mock import patch

from data_science_pipeline.utils.notebook import (
    get_default_state_path
)


class TestGetDefaultStatePath:
    @patch.dict(os.environ, {})
    def test_should_default_to_state_path_using_deployment_env(self):
        assert (
            get_default_state_path('test')
            == 's3://test-elife-data-pipeline/airflow-config/data-science/state'
        )

    @patch.dict(os.environ, {'DATA_SCIENCE_STATE_PATH': '/path/to/state'})
    def test_should_use_configured_state_path(self):
        assert (
            get_default_state_path('test')
            == '/path/to/state'
        )
