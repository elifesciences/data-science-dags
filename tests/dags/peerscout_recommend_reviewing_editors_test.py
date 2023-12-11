from airflow.models.baseoperator import DEFAULT_QUEUE

from dags.peerscout_recommend_reviewing_editors import (
    DATA_SCIENCE_PEERSCOUT_RECOMMEND_QUEUE_ENV_NAME,
    get_queue
)


class TestGetQueue:
    def test_should_return_default_queue_if_no_env_var_defined(self, mock_env: dict):
        mock_env.clear()
        assert get_queue() == DEFAULT_QUEUE

    def test_should_return_selected_queue_from_evn_var(self, mock_env: dict):
        mock_env[DATA_SCIENCE_PEERSCOUT_RECOMMEND_QUEUE_ENV_NAME] = 'queue1'
        assert get_queue() == 'queue1'
