from data_science_pipeline.utils.notebook import (
    get_default_state_path
)


class TestGetDefaultStatePath:
    def test_should_default_to_state_path_using_deployment_env(self, mock_env: dict):
        assert not mock_env.get('DATA_SCIENCE_STATE_PATH')
        assert (
            get_default_state_path('test')
            == 's3://test-elife-data-pipeline/airflow-config/data-science/state'
        )

    def test_should_use_configured_state_path(self, mock_env: dict):
        mock_env['DATA_SCIENCE_STATE_PATH'] = '/path/to/state'
        assert (
            get_default_state_path('test')
            == '/path/to/state'
        )
