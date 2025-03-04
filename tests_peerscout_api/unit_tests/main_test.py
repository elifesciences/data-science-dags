import logging
from typing import Iterable
from unittest.mock import ANY, patch, MagicMock

from flask.testing import FlaskClient

import pytest
import pandas as pd
# from werkzeug.wrappers.response import Response

from peerscout_api.main import (
    NOT_PROVIDED,
    SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE,
    create_app,
    get_html_text_for_recommended_person,
    get_html_text_for_author_suggested_person,
    get_keyword_extractor,
    get_list_of_author_suggested_person_details_with_html_text,
    PersonProps,
    get_recommendation_html,
    RECOMMENDATION_HEADINGS,
    NO_RECOMMENDATION_HTML,
    EDITOR_TYPE_FOR_REVIEWING_EDITOR,
    EDITOR_TYPE_FOR_SENIOR_EDITOR,
    get_spacy_keyword_extraction_api_url,
    pick_person_id_from_bq_result
)

import peerscout_api.main as target_module
from peerscout_api.spacy_api_keyword_extractor import SpaCyApiKeywordExtractor


LOGGER = logging.getLogger(__name__)

INPUT_DATA_VALID: dict = {
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

INPUT_DATA_WTIH_WEAK_ABSTRACT = {**INPUT_DATA_VALID, "abstract": "abc bcd efg"}

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

EDITOR_TYPE = 'EditorType1'

RECOMMENDED_PERSON_IDS = ['id1', 'id2', 'id3']
RECOMMENDED_NAMES = ['name1', 'name2', 'name3']
SUGGESTED_PERSON_IDS_TO_INC = ['id4', 'id5']
SUGGESTED_PERSON_IDS_TO_EXC = ['id6']

PERSON_NAME_HTML = "John Matt"
INSTITUTION_HTML = "University of Nowhereland"
COUNTRY_HTML = ", Nowhere"
AVAILABILITY_HTML = (
    "<br /><span style=\'color:red;\'><strong>!</strong></span>"
    + " Limited availability: Sundays only until 3th August 2021"
)
WEBSITE_HTML = '<a href="http://universityofnowhereland.edu" target=_blank>Website</a>'
PUBMED_HTML = '<a href="http://universityofnowherelandpubmed.edu" target=_blank>PubMed</a>'
DAYS_TO_RESPOND_HTML = "Days to respond: 0.9"
REQUESTS_HTML = "Requests: 13"
RESPONSES_HTML = "Responses: 12"
RESPONSE_RATE_HTML = "Response rate: 80%"
NO_OF_ASSIGMENTS_HTML = "No. of current assignments: 0"
NO_OF_FULL_SUBMISSIONS_HTML = "Full submissions in 12 months: 4"
DECISION_TIME_HTML = "Decision time: 43 days"

LINE_BREAK = '<br />'
SEMI_COLUMN = '; '
PIPE = ' | '

BQ_RESPONSE_DICT_1 = {'person_id': 'person_id_1'}
BQ_RESPONSE_DICT_2 = {'person_id': 'person_id_2'}


def get_valid_recommendation_response():
    return {
        "reviewing_editor_recommendation": {
            "person_ids": RECOMMENDED_PERSON_IDS,
            "recommendation_html": get_recommendation_html(
                author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
                author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
                recommended_person_ids=RECOMMENDED_PERSON_IDS,
                editor_type=EDITOR_TYPE_FOR_REVIEWING_EDITOR)
        },
        "senior_editor_recommendation": {
            "person_ids": RECOMMENDED_PERSON_IDS,
            "recommendation_html": get_recommendation_html(
                author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
                author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
                recommended_person_ids=RECOMMENDED_PERSON_IDS,
                editor_type=EDITOR_TYPE_FOR_SENIOR_EDITOR)
        }
    }


def get_valid_no_recommendation_response():
    return {
        "reviewing_editor_recommendation": {
            "person_ids": [],
            "recommendation_html": get_recommendation_html(
                author_suggestion_exclude_editor_ids=[],
                author_suggestion_include_editor_ids=[],
                recommended_person_ids=[],
                editor_type=EDITOR_TYPE_FOR_REVIEWING_EDITOR)
        },
        "senior_editor_recommendation": {
            "person_ids": [],
            "recommendation_html": get_recommendation_html(
                author_suggestion_exclude_editor_ids=[],
                author_suggestion_include_editor_ids=[],
                recommended_person_ids=[],
                editor_type=EDITOR_TYPE_FOR_SENIOR_EDITOR)
        }
    }


@pytest.fixture(name='query_bq_for_person_details_mock', autouse=True)
def _query_bq_for_person_details_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'query_bq_for_person_details') as mock:
        yield mock


@pytest.fixture(name='load_json_list_and_append_to_bq_table_with_auto_schema_mock', autouse=True)
def _load_json_list_and_append_to_bq_table_with_auto_schema_mock() -> Iterable[MagicMock]:
    with patch.object(
        target_module,
        'load_json_list_and_append_to_bq_table_with_auto_schema'
    ) as mock:
        yield mock


@pytest.fixture(name='get_editor_recommendations_for_api_mock', autouse=True)
def _get_editor_recommendations_for_api_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'get_editor_recommendations_for_api') as mock:
        mock.return_value = pd.DataFrame(columns=['person_id', 'name'])
        yield mock


@pytest.fixture(name='write_peerscout_api_response_to_bq_in_a_thread_mock')
def _write_peerscout_api_response_to_bq_in_a_thread_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'write_peerscout_api_response_to_bq_in_a_thread') as mock:
        yield mock


@pytest.fixture(name='get_keyword_extractor_mock', autouse=True)
def _get_keyword_extractor_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'get_keyword_extractor') as mock:
        yield mock


@pytest.fixture(name='keyword_extractor_mock', autouse=True)
def _keyword_extractor_mock(get_keyword_extractor_mock: MagicMock) -> MagicMock:
    return get_keyword_extractor_mock.return_value


@pytest.fixture(name='load_model_mock', autouse=True)
def _load_model_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'load_model') as mock:
        yield mock


@pytest.fixture(name='test_client')
def _test_client() -> FlaskClient:
    app = create_app()
    return app.test_client()


def _get_ok_json(response):
    assert response.status_code == 200
    return response.json


class TestGetSpacyKeywordExtractionApiUrl:
    def test_should_fail_if_not_configured(self, mock_env: dict):
        assert SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE not in mock_env
        with pytest.raises(KeyError):
            get_spacy_keyword_extraction_api_url()

    def test_should_return_configured_url(self, mock_env: dict):
        mock_env[SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE] = 'url_1'
        assert get_spacy_keyword_extraction_api_url() == 'url_1'


class TestPickPersonIdFromBqResult:
    def test_should_return_empty_result_if_passed_list_empty(self):
        assert not pick_person_id_from_bq_result(
            bq_person_detail_sql_result=[BQ_RESPONSE_DICT_1, BQ_RESPONSE_DICT_2],
            person_ids_to_pick=[]
        )

    def test_should_return_bq_result_for_list_of_one_person_id(self):
        actual_response = pick_person_id_from_bq_result(
            bq_person_detail_sql_result=[BQ_RESPONSE_DICT_1, BQ_RESPONSE_DICT_2],
            person_ids_to_pick=[BQ_RESPONSE_DICT_1['person_id']]
        )
        assert actual_response == [BQ_RESPONSE_DICT_1]

    def test_should_preserve_order_of_passed_in_person_ids(self):
        actual_response = pick_person_id_from_bq_result(
            bq_person_detail_sql_result=[BQ_RESPONSE_DICT_1, BQ_RESPONSE_DICT_2],
            person_ids_to_pick=[BQ_RESPONSE_DICT_2['person_id'], BQ_RESPONSE_DICT_1['person_id']]
        )
        assert actual_response == [BQ_RESPONSE_DICT_2, BQ_RESPONSE_DICT_1]


class TestGetKeywordExtractor:
    def test_should_return_spacy_api_keyword_extractor_with_api_url(
        self,
        mock_env: dict
    ):
        mock_env[SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE] = 'url_1'
        result = get_keyword_extractor()
        assert isinstance(result, SpaCyApiKeywordExtractor)
        assert result.api_url == 'url_1'


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
        assert _get_ok_json(response) == get_valid_no_recommendation_response()

    def test_should_respond_no_recomendation_with_weak_abstract(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json=INPUT_DATA_WTIH_WEAK_ABSTRACT)
        assert _get_ok_json(response) == get_valid_no_recommendation_response()

    def test_should_pass_abstract_to_keyword_extractor(
        self,
        test_client: FlaskClient,
        keyword_extractor_mock: MagicMock
    ):
        test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        keyword_extractor_mock.iter_extract_keywords.assert_called_with(
            text_list=[INPUT_DATA_VALID['abstract']]
        )

    def test_should_pass_extracted_keywords_into_get_editor_recommendations_for_api_func(
        self,
        test_client: FlaskClient,
        keyword_extractor_mock: MagicMock,
        get_editor_recommendations_for_api_mock: MagicMock
    ):
        keyword_extractor_mock.iter_extract_keywords.return_value = iter(
            [['keyword_1', 'keyword_2']]
        )
        test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        get_editor_recommendations_for_api_mock.assert_called_with(
                ANY,
                [['keyword_1', 'keyword_2']],
                ANY
        )

    def test_should_respond_with_recomendation(
        self,
        test_client: FlaskClient,
        get_editor_recommendations_for_api_mock: MagicMock
    ):
        get_editor_recommendations_for_api_mock.return_value = pd.DataFrame(
            {'person_id': RECOMMENDED_PERSON_IDS, 'name': RECOMMENDED_NAMES}
        )
        response = test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        assert _get_ok_json(response) == get_valid_recommendation_response()

    def test_should_call_write_peerscout_api_response_to_bq_in_a_thread_with_correct_params(
        self,
        test_client: FlaskClient,
        write_peerscout_api_response_to_bq_in_a_thread_mock: MagicMock
    ):
        response = test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        write_peerscout_api_response_to_bq_in_a_thread_mock.assert_called_with(
            recommendation_request=INPUT_DATA_VALID,
            recommendation_response=_get_ok_json(response)
        )


class TestGetRecommendationHtml:
    def test_should_have_recomendation_heading_when_the_recomendation_not_avaliable(
        self
    ):
        result_recommendation_html = get_recommendation_html(
                author_suggestion_exclude_editor_ids=[],
                author_suggestion_include_editor_ids=[],
                recommended_person_ids=[],
                editor_type=EDITOR_TYPE
        )
        for heading in RECOMMENDATION_HEADINGS:
            heading = heading.format(editor_type=EDITOR_TYPE)
            assert heading in result_recommendation_html

    def test_should_have_recomendation_heading_when_the_recomendation_avaliable(
        self
    ):
        result_recommendation_html = get_recommendation_html(
            author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
            author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
            recommended_person_ids=RECOMMENDED_PERSON_IDS,
            editor_type=EDITOR_TYPE
        )
        for heading in RECOMMENDATION_HEADINGS:
            heading = heading.format(editor_type=EDITOR_TYPE)
            assert heading in result_recommendation_html

    def test_should_not_contain_line_feed_in_recommendation_html(self):
        result_recommendation_html = get_recommendation_html(
            author_suggestion_exclude_editor_ids=SUGGESTED_PERSON_IDS_TO_EXC,
            author_suggestion_include_editor_ids=SUGGESTED_PERSON_IDS_TO_INC,
            recommended_person_ids=RECOMMENDED_PERSON_IDS,
            editor_type=EDITOR_TYPE
        )
        assert "\n" not in result_recommendation_html


class TestGetHtmlTextForRecommendedPerson:
    def test_should_have_person_name_if_there_is_no_other_information(self):
        person = PersonProps(
            person_name='John Matt'
        )
        expected_result_of_html = PERSON_NAME_HTML
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_all_fields_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            country='Nowhere',
            availability='Sundays only until 3th August 2021',
            website='http://universityofnowhereland.edu',
            pubmed='http://universityofnowherelandpubmed.edu',
            days_to_respond='0.9',
            requests='13',
            responses='12',
            response_rate='80',
            no_of_assigments='0',
            no_of_full_submissions='4',
            decision_time='43'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + INSTITUTION_HTML
            + COUNTRY_HTML
            + AVAILABILITY_HTML
            + LINE_BREAK
            + WEBSITE_HTML
            + PIPE
            + PUBMED_HTML
            + LINE_BREAK
            + DAYS_TO_RESPOND_HTML
            + SEMI_COLUMN
            + REQUESTS_HTML
            + SEMI_COLUMN
            + RESPONSES_HTML
            + SEMI_COLUMN
            + RESPONSE_RATE_HTML
            + LINE_BREAK
            + NO_OF_ASSIGMENTS_HTML
            + SEMI_COLUMN
            + NO_OF_FULL_SUBMISSIONS_HTML
            + SEMI_COLUMN
            + DECISION_TIME_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_website_pubmed_if_they_are_provided(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland',
            website='http://universityofnowhereland.edu',
            pubmed='http://universityofnowherelandpubmed.edu',
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + INSTITUTION_HTML
            + LINE_BREAK
            + WEBSITE_HTML
            + PIPE
            + PUBMED_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_stats_for_initial_submission_provided(self):
        person = PersonProps(
            person_name='John Matt',
            days_to_respond='0.9',
            requests='13',
            responses='12',
            response_rate='80'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + DAYS_TO_RESPOND_HTML
            + SEMI_COLUMN
            + REQUESTS_HTML
            + SEMI_COLUMN
            + RESPONSES_HTML
            + SEMI_COLUMN
            + RESPONSE_RATE_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_response_rate_even_the_other_stats_not_provided(self):
        person = PersonProps(
            person_name='John Matt',
            response_rate='80'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + RESPONSE_RATE_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_stats_for_full_submission_provided(self):
        person = PersonProps(
            person_name='John Matt',
            no_of_assigments='0',
            no_of_full_submissions='4',
            decision_time='43'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + NO_OF_ASSIGMENTS_HTML
            + SEMI_COLUMN
            + NO_OF_FULL_SUBMISSIONS_HTML
            + SEMI_COLUMN
            + DECISION_TIME_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html

    def test_should_have_decision_time_even_the_other_stats_not_provided(self):
        person = PersonProps(
            person_name='John Matt',
            decision_time='43'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + LINE_BREAK
            + DECISION_TIME_HTML
        )
        assert get_html_text_for_recommended_person(person) == expected_result_of_html


class TestGetHtmlTextForAuthorSuggestedPerson:
    def test_should_have_person_name_and_institution(self):
        person = PersonProps(
            person_name='John Matt',
            institution='University of Nowhereland'
        )
        expected_result_of_html = (
            PERSON_NAME_HTML
            + SEMI_COLUMN
            + INSTITUTION_HTML
        )
        assert get_html_text_for_author_suggested_person(person) == expected_result_of_html

    def test_should_display_not_provided_when_there_is_no_author_suggestion(self):
        expected_result_of_html = (
            NOT_PROVIDED
        )
        assert (
            get_list_of_author_suggested_person_details_with_html_text([])
            ==
            expected_result_of_html
        )

    def test_should_only_have_person_name_if_there_is_no_institution(self):
        person = PersonProps(
            person_name='John Matt'
        )
        expected_result_of_html = PERSON_NAME_HTML
        assert get_html_text_for_author_suggested_person(person) == expected_result_of_html
