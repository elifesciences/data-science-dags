import logging
import re
from abc import ABC, abstractmethod
from typing import Iterable, List

import spacy
from spacy.language import Language

from elife_data_hub_utils.keyword_extract.spacy_keyword import (
    SpacyKeywordDocumentParser,
    DEFAULT_SPACY_LANGUAGE_MODEL_NAME
)


LOGGER = logging.getLogger(__name__)


class KeywordExtractor(ABC):
    @abstractmethod
    def iter_extract_keywords(
            self, text_list: Iterable[str]) -> Iterable[List[str]]:
        pass


def simple_regex_keyword_extraction(
        text: str,
        regex_pattern=r"([a-z](?:\w|-)+)"
):
    return re.findall(regex_pattern, text.lower())


class SimpleKeywordExtractor(KeywordExtractor):
    def iter_extract_keywords(
            self, text_list: Iterable[str]) -> Iterable[List[str]]:
        return (
            simple_regex_keyword_extraction(text)
            for text in text_list
        )


class SpacyKeywordExtractor(KeywordExtractor):
    def __init__(self, language: Language):
        self.parser = SpacyKeywordDocumentParser(language)

    def iter_extract_keywords(
            self, text_list: Iterable[str]) -> Iterable[List[str]]:
        return (
            document.get_keyword_str_list()
            for document in self.parser.iter_parse_text_list(text_list)
        )


def get_keyword_extractor(spacy_language_model: str) -> KeywordExtractor:
    LOGGER.info(
        'loading keyword extractor, spacy language model: %s',
        spacy_language_model
    )
    extractor = SimpleKeywordExtractor()
    if spacy_language_model:
        spacy_language_model_name = (
            spacy_language_model
            or DEFAULT_SPACY_LANGUAGE_MODEL_NAME
        )
        extractor = SpacyKeywordExtractor(spacy.load(
            spacy_language_model_name
        ))
    return extractor
