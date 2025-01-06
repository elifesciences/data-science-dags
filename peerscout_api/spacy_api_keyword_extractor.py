from dataclasses import dataclass
from typing import Iterable, List

import requests

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    KeywordExtractor
)


DEFAULT_TIMEOUT = 60


def get_request_body(_text_list: Iterable[str]) -> dict:
    return {}


@dataclass(frozen=True)
class SpaCyApiKeywordExtractor(KeywordExtractor):
    api_url: str

    def iter_extract_keywords(
        self,
        text_list: Iterable[str]
    ) -> Iterable[List[str]]:
        requests.post(
            url=self.api_url,
            json=get_request_body(text_list),
            timeout=DEFAULT_TIMEOUT
        )
        return []
