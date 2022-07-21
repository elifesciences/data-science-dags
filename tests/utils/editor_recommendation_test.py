import pandas as pd

from data_science_pipeline.utils.editor_recommendation import (
    get_author_ids_of_given_version_of_paper
)

MANUSCRIPT_VERSION_1 = "match_version_id_1"

AUTHOR_ID_LIST_1 = [
    "match_author_id1",
    "match_author_id2"
]

MANUSCRIPT_VERSION_2 = "unmatch_version_id_1"

AUTHOR_ID_LIST_2 = [
    "unmatch_author_id1"
]

MANUSCRIPT_DICT = {
  "version_id": {
    "0": MANUSCRIPT_VERSION_1,
    "1": MANUSCRIPT_VERSION_2
  },
  "author_person_ids": {
    "0": AUTHOR_ID_LIST_1,
    "1": AUTHOR_ID_LIST_2
  }
}

MANUSCRIPT_DF = pd.DataFrame(MANUSCRIPT_DICT)

print(MANUSCRIPT_DF)

class TestGetAuthorIdsOfGivenVersionOfPaper:
    def test_should_return_author_ids_for_matching_manuscript_version(self):
        assert get_author_ids_of_given_version_of_paper(
            MANUSCRIPT_DF,
            MANUSCRIPT_VERSION_1
        ) == AUTHOR_ID_LIST_1

    def test_should_not_return_author_ids_for_unmatching_manuscript_version(self):
        assert get_author_ids_of_given_version_of_paper(
            MANUSCRIPT_DF,
            MANUSCRIPT_VERSION_2
        ) != AUTHOR_ID_LIST_1
