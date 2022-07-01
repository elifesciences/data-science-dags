from typing import NamedTuple


class BigQueryTargetConfig(NamedTuple):
    project_name: str
    dataset_name: str
    table_name: str

    @staticmethod
    def from_dict(target_config_dict: dict) -> 'BigQueryTargetConfig':
        return BigQueryTargetConfig(
            project_name=target_config_dict['projectName'],
            dataset_name=target_config_dict['datasetName'],
            table_name=target_config_dict['tableName']
        )


class PeerscoutApiConfig(NamedTuple):
    target: BigQueryTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'PeerscoutApiConfig':
        return PeerscoutApiConfig(
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'PeerscoutApiConfig':
        item_config_list = config_dict['peerscoutApi']
        return PeerscoutApiConfig._from_item_dict(
            item_config_list[0]
        )
