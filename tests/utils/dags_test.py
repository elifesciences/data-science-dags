import os
from unittest.mock import patch

from data_science_pipeline.utils.dags import (
    get_default_state_path,
    get_default_notebook_task_id
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


class TestGetDefaultNotebookTaskId:
    def test_should_return_passed_in_value_without_special_characters(self):
        assert get_default_notebook_task_id('test123') == 'test123'

    def test_should_strip_ext(self):
        assert get_default_notebook_task_id('test123.ext') == 'test123'

    def test_should_strip_dirname(self):
        assert get_default_notebook_task_id('/path/to/test123.ext') == 'test123'

    def test_should_replace_special_characters_with_underscore(self):
        assert get_default_notebook_task_id('test!123.ext') == 'test_123'
