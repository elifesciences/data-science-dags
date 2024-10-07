import pandas as pd

from data_science_pipeline.utils.editor_recommendation import (
    get_author_ids_of_given_version_of_manuscript
)

MANUSCRIPT_VERSION_ID_1 = "manuscript_version_id_1"
AUTHOR_ID_LIST_1 = [
    "manuscript_version_id_1_author_id1",
    "manuscript_version_id_1_author_id2"
]

MANUSCRIPT_VERSION_ID_2 = "manuscript__version_id_2"
AUTHOR_ID_LIST_2 = ["manuscript__version_id_2_author_id1"]

MANUSCRIPT_VERSION_DF = pd.DataFrame(
    [{
        'version_id': MANUSCRIPT_VERSION_ID_1,
        'author_person_ids': AUTHOR_ID_LIST_1
    }, {
        'version_id': MANUSCRIPT_VERSION_ID_2,
        'author_person_ids': AUTHOR_ID_LIST_2
    }]
)


class TestGetAuthorIdsOfGivenVersionOfManuscript:
    def test_should_return_matching_author_ids_for_given_manuscript_version_id(self):
        assert get_author_ids_of_given_version_of_manuscript(
            MANUSCRIPT_VERSION_DF,
            MANUSCRIPT_VERSION_ID_1
        ) == AUTHOR_ID_LIST_1

    def test_should_return_matching_author_ids_for_another_given_manuscript_version_id(self):
        assert get_author_ids_of_given_version_of_manuscript(
            MANUSCRIPT_VERSION_DF,
            MANUSCRIPT_VERSION_ID_2
        ) == AUTHOR_ID_LIST_2
