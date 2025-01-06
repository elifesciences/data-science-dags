from dataclasses import dataclass
from typing import Iterable, List

import requests

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    KeywordExtractor
)


DEFAULT_TIMEOUT = 60


@dataclass(frozen=True)
class SpaCyApiKeywordExtractor(KeywordExtractor):
    api_url: str

    def iter_extract_keywords(
        self,
        text_list: Iterable[str]
    ) -> Iterable[List[str]]:
        requests.post(
            url=self.api_url,
            timeout=DEFAULT_TIMEOUT
        )
        return []
