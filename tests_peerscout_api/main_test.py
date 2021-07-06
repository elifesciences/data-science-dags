import logging
from unittest.mock import patch, MagicMock

from flask.testing import FlaskClient

import pytest
import pandas as pd
# from werkzeug.wrappers.response import Response

from peerscout_api.main import (
    create_app,
    get_html_text_for_recommended_person,
    get_html_text_for_author_suggested_person,
    PersonProps,
    get_recommendation_html,
    RECOMMENDATION_HEADINGS,
    NO_RECOMMENDATION_HTML
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

NO_RECOMMENDATION_RESPONSE = {
    "reviewing_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": NO_RECOMMENDATION_HTML
    },
    "senior_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": NO_RECOMMENDATION_HTML
    }
}

RECOMMENDED_PERSON_IDS = ['id1', 'id2', 'id3']
RECOMMENDED_NAMES = ['name1', 'name2', 'name3']
SUGGESTED_PERSON_IDS_TO_INC = ['id4', 'id5']
SUGGESTED_PERSON_IDS_TO_EXC = ['id6']


VALID_RECOMMENDATION_RESPONSE = {
    "reviewing_editor_recommendation": {
        "person_ids": RECOMMENDED_PERSON_IDS,
        "recommendation_html": get_recommendation_html(
            author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
            author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
            recommended_person_ids=RECOMMENDED_PERSON_IDS)
    },
    "senior_editor_recommendation": {
        "person_ids": RECOMMENDED_PERSON_IDS,
        "recommendation_html": get_recommendation_html(
            author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
            author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
            recommended_person_ids=RECOMMENDED_PERSON_IDS)
    }
}


@pytest.fixture(name='query_bq_for_person_details_mock', autouse=True)
def _query_bq_for_person_details_mock() -> MagicMock:
    with patch.object(target_module, 'query_bq_for_person_details') as mock:
        yield mock


@pytest.fixture(name='get_editor_recommendations_for_api_mock', autouse=True)
def _get_editor_recommendations_for_api_mock() -> MagicMock:
    with patch.object(target_module, 'get_editor_recommendations_for_api') as mock:
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
        assert _get_ok_json(response) == NO_RECOMMENDATION_RESPONSE

    def test_should_respond_with_recomendation(
        self,
        test_client: FlaskClient,
        get_editor_recommendations_for_api_mock: MagicMock
    ):
        get_editor_recommendations_for_api_mock.return_value = pd.DataFrame(
            {'person_id': RECOMMENDED_PERSON_IDS, 'name': RECOMMENDED_NAMES}
        )
        response = test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        assert _get_ok_json(response) == VALID_RECOMMENDATION_RESPONSE


class TestGetRecommendationHtml:
    def test_should_have_recomendation_heading_when_the_recomendation_not_avaliable(
        self
    ):
        for heading in RECOMMENDATION_HEADINGS:
            assert heading in get_recommendation_html(
                author_suggestion_exclude_editor_ids=[],
                author_suggestion_include_editor_ids=[],
                recommended_person_ids=[])

    def test_should_have_recomendation_heading_when_the_recomendation_avaliable(
        self
    ):
        for heading in RECOMMENDATION_HEADINGS:
            assert heading in get_recommendation_html(
                author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
                author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
                recommended_person_ids=RECOMMENDED_PERSON_IDS)


# pylint: disable=line-too-long
class TestGetHtmlTextForRecommendedPerson:
    def test_should_have_person_name_if_there_is_no_other_information(self):
        person = PersonProps(
            person_name='John Matt'
        )
        expected_result_of_html = """<p>John Matt</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_all_fields_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            country='Nowhere',
            availability='Sundays only until 3th August 2021',
            Website_URL='http://universityofnowhereland.edu',
            PubMed_URL='http://universityofnowherelandpubmed.edu',
            days_to_respond='0.9',
            requests='13',
            responses='12',
            response_rate='80',
            no_of_assigments='0',
            no_full_submissions='4',
            decision_time='43'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland, Nowhere<br /><span style=\'color:red;\'><strong>!</strong></span> Limited availability: Sundays only until 3th August 2021<br /><a href=http://universityofnowhereland.edu>Website</a> | <a href=http://universityofnowherelandpubmed.edu>PubMed</a><br />Days to respond: 0.9; Requests: 13; Responses: 12; Response rate: 80%<br />No. of current assignments: 0; Full submissions in 12 months: 4; Decision time: 43 days</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_website_if_it_is_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            country='Nowhere',
            availability='Sundays only until 3th August 2021',
            Website_URL='http://universityofnowhereland.edu'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland, Nowhere<br /><span style=\'color:red;\'><strong>!</strong></span> Limited availability: Sundays only until 3th August 2021<br /><a href=http://universityofnowhereland.edu>Website</a></p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_website_pubmed_if_they_are_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            Website_URL='http://universityofnowhereland.edu',
            PubMed_URL='http://universityofnowherelandpubmed.edu',
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland<br /><a href=http://universityofnowhereland.edu>Website</a> | <a href=http://universityofnowherelandpubmed.edu>PubMed</a></p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_stats_for_initial_submission_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            country='Nowhere',
            days_to_respond='0.9',
            requests='13',
            responses='12',
            response_rate='80'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland, Nowhere<br />Days to respond: 0.9; Requests: 13; Responses: 12; Response rate: 80%</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_response_rate_even_the_other_stats_not_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            country='Nowhere',
            response_rate='80'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland, Nowhere<br />Response rate: 80%</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_stats_for_full_submission_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            requests='13',
            responses='12',
            response_rate='80',
            no_of_assigments='0',
            no_full_submissions='4',
            decision_time='43'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland<br />Requests: 13; Responses: 12; Response rate: 80%<br />No. of current assignments: 0; Full submissions in 12 months: 4; Decision time: 43 days</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_decision_time_even_the_other_stats_not_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            decision_time='43'
        )
        expected_result_of_html = """<p>John Matt<br />University of Nowhereland<br />Decision time: 43 days</p>"""
        assert get_html_text_for_recommended_person(person) == expected_result_of_html


class TestGetHtmlTextForAuthorSuggestedPerson:
    def test_should_have_person_name_and_institution(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland'
        )
        expected_result_of_html = 'John Matt; University of Nowhereland'
        assert get_html_text_for_author_suggested_person(person) == expected_result_of_html

    def test_should_only_have_person_name_if_there_is_no_institution(self):
        person = PersonProps(
            person_name='John Matt'
        )
        expected_result_of_html = 'John Matt'
        assert get_html_text_for_author_suggested_person(person) == expected_result_of_html
