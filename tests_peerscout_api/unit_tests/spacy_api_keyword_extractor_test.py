

from typing import Iterable
from unittest.mock import ANY, MagicMock, patch
import pytest

import peerscout_api.spacy_api_keyword_extractor as target_module
from peerscout_api.spacy_api_keyword_extractor import (
    SpaCyApiKeywordExtractor,
    get_batch_keywords_from_response,
    get_request_body
)


TEST_SPACY_API_URL_1 = 'http://example/spacy-url-1'


@pytest.fixture(name='requests_mock', autouse=True)
def _requests_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'requests') as mock:
        yield mock


@pytest.fixture(name='requests_post_mock', autouse=True)
def _requests_post_mock(requests_mock: MagicMock) -> MagicMock:
    return requests_mock.post


@pytest.fixture(name='response_mock', autouse=True)
def _response_mock(requests_post_mock: MagicMock) -> MagicMock:
    return requests_post_mock.return_value


class TestGetRequestBody:
    def test_should_return_keyword_request_body(self):
        assert get_request_body(['text_1']) == {
            'data': [{
                'type': 'extract-keyword-request',
                'attributes': {
                    'content': 'text_1'
                }
            }]
        }


class TestSpaCyApiKeywordExtractor:
    def test_should_call_api(self, requests_post_mock: MagicMock):
        keyword_extractor = SpaCyApiKeywordExtractor(api_url=TEST_SPACY_API_URL_1)
        keyword_extractor.iter_extract_keywords(text_list=['text_1'])
        requests_post_mock.assert_called_with(
            url=TEST_SPACY_API_URL_1,
            json=get_request_body(['text_1']),
            timeout=ANY
        )

    def test_should_get_keywords_from_response(self, response_mock: MagicMock):
        keyword_extractor = SpaCyApiKeywordExtractor(api_url=TEST_SPACY_API_URL_1)
        response_mock.json.return_value = {}
        extracted_keywords = list(keyword_extractor.iter_extract_keywords(text_list=['text_1']))
        assert extracted_keywords == get_batch_keywords_from_response({})
