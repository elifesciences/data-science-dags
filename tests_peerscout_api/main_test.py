import logging
from unittest.mock import patch, MagicMock

from flask.testing import FlaskClient

import pytest
import pandas as pd

from peerscout_api.main import (
    create_app,
    # get_recommendation_html,
    # RECOMENDATION_HTML,
    # RECOMMENDATION_HEADINGS,
    NO_RECOMENDATION_HTML
)

import peerscout_api.main as target_module


LOGGER = logging.getLogger(__name__)

INPUT_DATA_VALID = {
    "manuscript_id": "12345",
    "tracking_number": "12345_ABC",
    "revision_number": 0,
    "abstract": "<p>Lorem ipsum dolor sit amet, <i>consectetur adipiscing elit</i></p>",
    "title": "<b>Lorem Ipsum</b>",
    "author_suggestion": {
      "include_reviewing_editors_id": [
        "43038",
        "178962",
        "7970"
      ],
      "exclude_reviewing_editors_id": [
        "1000"
      ],
      "include_senior_editors_id": [
        "15332",
        "42473",
        "42011"
      ],
      "exclude_senior_editors_id": [
        "73636"
      ]
    }
}

INPUT_DATA_WTIHOUT_ABSTRACT = {**INPUT_DATA_VALID, "abstract": ""}

NO_RECOMENDATION_RESPONSE = {
    "reviewing_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": NO_RECOMENDATION_HTML
    },
    "senior_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": NO_RECOMENDATION_HTML
    }
}

RECOMMENDED_PERSON_IDS = ['1', '2', '3']
RECOMMENDED_NAMES = ['A', 'B', 'C']
AUTHOR_SUGGESTED_IDS = ['5']

# FORMATED_SUGGESTED_NAME = "<br />D"
# FORMATED_RECOMMENDED_NAME = ['<br />A', '<br />B', '<br />C']

# VALID_RECOMENDATION_RESPONSE = {
#     "reviewing_editor_recommendation": {
#         "person_ids": RECOMMENDED_PERSON_IDS,
#         "recommendation_html": get_recommendation_html(
#             recommended_person_ids=RECOMMENDED_PERSON_IDS,
#             recommended_names=RECOMMENDED_NAMES,
#             author_suggestion_exclude_editor_ids=AUTHOR_SUGGESTED_IDS,
#             author_suggestion_include_editor_ids=AUTHOR_SUGGESTED_IDS)
#     },
#     "senior_editor_recommendation": {
#         "person_ids": RECOMMENDED_PERSON_IDS,
#         "recommendation_html": get_recommendation_html(
#             recommended_person_ids=RECOMMENDED_PERSON_IDS,
#             recommended_names=RECOMMENDED_NAMES,
#             author_suggestion_exclude_editor_ids=AUTHOR_SUGGESTED_IDS,
#             author_suggestion_include_editor_ids=AUTHOR_SUGGESTED_IDS)
#     }
# }


@pytest.fixture(name='get_person_names_from_bq_mock', autouse=True)
def _get_person_names_from_bq_mock() -> MagicMock:
    with patch.object(target_module, 'get_person_names_from_bq') as mock:
        yield mock


# @pytest.fixture(name='get_formated_html_text_mock', autouse=True)
# def _get_formated_html_text_mock() -> MagicMock:
#     with patch.object(target_module, 'get_formated_html_text') as mock:
#         mock.return_value = RECOMENDATION_HTML.format(
#             formated_excluded_name=FORMATED_SUGGESTED_NAME,
#             formated_included_name=FORMATED_SUGGESTED_NAME,
#             formated_recomended_name=FORMATED_RECOMMENDED_NAME)
#         yield mock


@pytest.fixture(name='get_editor_recomendations_for_api_mock', autouse=True)
def _get_editor_recomendations_for_api_mock() -> MagicMock:
    with patch.object(target_module, 'get_editor_recomendations_for_api') as mock:
        mock.return_value = pd.DataFrame(columns=['person_id', 'name'])
        yield mock


@pytest.fixture(name='get_keyword_extractor_mock', autouse=True)
def _get_keyword_extractor_mock() -> MagicMock:
    with patch.object(target_module, 'get_keyword_extractor') as mock:
        yield mock


@pytest.fixture(name='load_model_mock', autouse=True)
def _load_model_mock() -> MagicMock:
    with patch.object(target_module, 'load_model') as mock:
        yield mock


@pytest.fixture(name='test_client')
def _test_client() -> FlaskClient:
    app = create_app()
    return app.test_client()


def _get_ok_json(response):
    assert response.status_code == 200
    return response.json


class TestPeerscoutAPI:
    def test_should_have_access_for_status_page(self, test_client: FlaskClient):
        response = test_client.get('/api/status')
        assert _get_ok_json(response) == {"status": "OK"}

    def test_should_respond_badrequest_with_empty_json(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json={})
        assert response.status_code == 400

    def test_should_respond_no_recomendation_with_empty_abstract(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json=INPUT_DATA_WTIHOUT_ABSTRACT)
        assert _get_ok_json(response) == NO_RECOMENDATION_RESPONSE

    # def test_should_respond_with_recomendation(
    #     self,
    #     test_client: FlaskClient,
    #     get_editor_recomendations_for_api_mock: MagicMock
    # ):
    #     get_editor_recomendations_for_api_mock.return_value = pd.DataFrame(
    #         {'person_id': RECOMMENDED_PERSON_IDS, 'name': RECOMMENDED_NAMES}
    #     )
    #     response = test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
    #     assert _get_ok_json(response) == VALID_RECOMENDATION_RESPONSE


# class TestGetRecommendationHtml:
#     # def test_should_have_editor_exclusion_when_the_recomendation_not_avaliable():
#     def test_should_have_recomendation_heading_when_the_recomendation_not_avaliable(
#         self
#     ):
#         for heading in RECOMMENDATION_HEADINGS:
#             assert heading in get_recommendation_html(
#                 recommended_person_ids=[],
#                 recommended_names=[],
#                 author_suggestion_exclude_editor_ids=[],
#                 author_suggestion_include_editor_ids=[])

#     def test_should_have_recomendation_heading_when_the_recomendation_avaliable(
#         self
#     ):
#         for heading in RECOMMENDATION_HEADINGS:
#             assert heading in get_recommendation_html(
#                 recommended_person_ids=RECOMMENDED_PERSON_IDS,
#                 recommended_names=RECOMMENDED_NAMES,
#                 author_suggestion_exclude_editor_ids=AUTHOR_SUGGESTED_IDS,
#                 author_suggestion_include_editor_ids=AUTHOR_SUGGESTED_IDS)
