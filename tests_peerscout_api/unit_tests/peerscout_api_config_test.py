from peerscout_api.peerscout_api_config import PeerscoutApiConfig

PROJECT_NAME = 'project_1'
DATASET_NAME = 'dataset_1'
TABLE_NAME = 'table_1'

TARGET_CONFIG = {
    'projectName': PROJECT_NAME,
    'datasetName': DATASET_NAME,
    'tableName': TABLE_NAME
}

ITEM_CONFIG_DICT = {
    'target': TARGET_CONFIG
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'peerscoutApi': [item_dict]}


CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT)


class TestPeerscoutApiConfig:
    def test_should_read_target_project_dataset_and_table_name(self):
        config = PeerscoutApiConfig.from_dict(CONFIG_DICT)
        assert config.target.project_name == PROJECT_NAME
        assert config.target.dataset_name == DATASET_NAME
        assert config.target.table_name == TABLE_NAME
