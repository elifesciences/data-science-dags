from dataclasses import dataclass
from typing import Iterable, List

import requests

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    KeywordExtractor
)


DEFAULT_TIMEOUT = 60


def get_request_body(text_list: Iterable[str]) -> dict:
    return {
        'data': [
            {
                'type': 'extract-keyword-request',
                'attributes': {
                    'content': text
                }
            }
            for text in text_list
        ]
    }


def get_batch_keywords_from_response(response_json: dict) -> List[List[str]]:
    return [
        [
            keyword_dict['keyword']
            for keyword_dict in keyword_result['attributes']['keywords']
        ]
        for keyword_result in response_json['data']
    ]


@dataclass(frozen=True)
class SpaCyApiKeywordExtractor(KeywordExtractor):
    api_url: str

    def iter_extract_keywords(
        self,
        text_list: Iterable[str]
    ) -> Iterable[List[str]]:
        response = requests.post(
            url=self.api_url,
            json=get_request_body(text_list),
            timeout=DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        return get_batch_keywords_from_response(response.json())
