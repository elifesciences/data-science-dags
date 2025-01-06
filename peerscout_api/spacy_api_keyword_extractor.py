from dataclasses import dataclass


@dataclass(frozen=True)
class SpaCyApiKeywordExtractor():
    api_url: str
