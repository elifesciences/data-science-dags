import logging
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from itertools import groupby
from typing import Any, Dict, List, Generic, Optional, Tuple, Union, TypeVar, cast

import numpy as np
import pandas as pd
from scipy.sparse import csc_matrix

from data_science_pipeline.utils.pandas import isnull


LOGGER = logging.getLogger(__name__)
T = TypeVar('T')
KT = TypeVar('KT')


def _to_list(a: list) -> list:
    if isnull(a):
        return []
    return a


def _get_index(a: Union[list, pd.Series]):
    if isinstance(a, (pd.Series, pd.DataFrame)):
        return a.index
    return range(len(a))


def _sorted_matching_keywords_map_list(
        matching_keywords_map_list: List[Dict[KT, List[Tuple[float, str]]]]
        ) -> List[Dict[KT, List[List[Tuple[float, str]]]]]:
    return [
        {
            key: sorted(
                matching_keywords,  # type: ignore
                key=lambda score_keyword: (-cast(float, score_keyword[0]), score_keyword[1])
            )
            for key, matching_keywords in matching_keywords_map.items()
        }
        for matching_keywords_map in matching_keywords_map_list
    ]


class Vectorizer(ABC):
    """Vectorizer for typing, e.g. sklearn's TfidfVectorizer"""

    @abstractmethod
    def transform(self, raw_documents: List[List[str]]) -> csc_matrix:
        pass

    @abstractmethod
    def get_feature_names(self) -> List[str]:
        pass


def get_weighted_keywords_list_for_tf_matrix(
        tf_matrix: np.ndarray,
        tf_keywords: List[str]) -> List[List[Tuple[float, str]]]:
    tf_matrix = np.asmatrix(tf_matrix)
    LOGGER.debug('tf_matrix: %s', tf_matrix)
    non_zero_matrix = np.nonzero(tf_matrix)
    LOGGER.debug('non_zero_matrix: %s', non_zero_matrix)
    tf_values = np.asarray(tf_matrix[non_zero_matrix])[0]
    LOGGER.debug('tf_values: %s', tf_values)
    flat_matching_keywords = np.asarray(tf_keywords)[non_zero_matrix[-1]]
    weighted_keywords_by_index = {
        key: [t[1] for t in grouped_values]
        for key, grouped_values in groupby(
            zip(non_zero_matrix[0], zip(tf_values, flat_matching_keywords)),
            key=lambda t: t[0]
        )
    }
    weighted_keywords_list = [
        weighted_keywords_by_index.get(i, [])
        for i in range(len(tf_matrix))
    ]
    return weighted_keywords_list


def get_count_weighted_keywords_list(
        keywords_list: List[List[str]]) -> List[List[Tuple[float, str]]]:
    return [
        [(count, keyword) for keyword, count in Counter(keywords).items()]
        for keywords in keywords_list
    ]


def get_weighted_keywords_list_for_vectorizer(
        keywords_list: List[List[str]],
        vectorizer: Optional[Vectorizer] = None) -> List[List[Tuple[float, str]]]:
    if vectorizer is None:
        return get_count_weighted_keywords_list(keywords_list)
    return get_weighted_keywords_list_for_tf_matrix(
        vectorizer.transform(keywords_list).todense(),
        vectorizer.get_feature_names()
    )


class WeightedKeywordModelRankingList(Generic[KT, T]):
    def __init__(
            self,
            matching_keywords_map_list: List[Dict[int, List[Tuple[float, str]]]],
            choices: List[T]):
        self.matching_keywords_map_list = _sorted_matching_keywords_map_list(
            matching_keywords_map_list
        )
        self.score_map_list = [
            {
                key: sum(
                    score  # type: ignore
                    for score, _ in matching_keywords
                )
                for key, matching_keywords in matching_keywords_map.items()
            }
            for matching_keywords_map in self.matching_keywords_map_list
        ]
        self.sorted_indices_list = [
            pd.Series(score_map).sort_values(ascending=False).index
            for score_map in self.score_map_list
        ]
        self.choices = choices

    def __repr__(self):
        return '%s(%r, choices=%r)' % (
            type(self).__name__,
            self.matching_keywords_map_list,
            self.choices
        )

    def get_ranked_choices_list(self, limit: Optional[int] = None) -> List[List[T]]:
        return [
            [
                self.choices[index]
                for index in sorted_indices[:limit]
            ]
            for sorted_indices in self.sorted_indices_list
        ]

    @property
    def ranked_choices_list(self) -> List[List[T]]:
        return self.get_ranked_choices_list()

    def get_ranked_scores_list(self, limit: Optional[int] = None) -> List[List[Dict[str, Any]]]:
        return [
            [
                {
                    'index': index,
                    'value': self.choices[index],
                    'score': score_map[index],
                    'matching_keywords': matching_keywords_map[index]
                }
                for index in sorted_indices[:limit]
            ]
            for sorted_indices, score_map, matching_keywords_map in zip(
                self.sorted_indices_list,
                self.score_map_list,
                self.matching_keywords_map_list
            )
        ]

    @property
    def ranked_scores_list(self) -> List[List[Dict[str, List[List[Tuple[float, str]]]]]]:
        return self.get_ranked_scores_list()

    def get_matching_keywords_list(
        self, limit: Optional[int] = None
    ) -> List[List[List[List[Tuple[float, str]]]]]:
        return [
            [
                matching_keywords_map.get(index, [])[:limit]
                for index in range(len(self.choices))
            ]
            for matching_keywords_map in self.matching_keywords_map_list
        ]

    @property
    def matching_keywords_list(self) -> List[List[List[List[Tuple[float, str]]]]]:
        return self.get_matching_keywords_list()

    @property
    def proba_matrix(self) -> List[List[float]]:
        return np.asmatrix([  # type: ignore
            [
                score_map.get(index, 0.0)
                for index in range(len(self.choices))
            ]
            for score_map in self.score_map_list
        ])


class WeightedKeywordModel:
    def __init__(
            self,
            choices: List[T],
            keywords_list: List[List[str]],
            keyword_weights_list: List[List[float]],
            vectorizer: Optional[Vectorizer] = None):
        assert len(choices) == len(keywords_list), "choices and keywords list must have same length"
        assert len(choices) == len(keyword_weights_list), \
            "choices and keyword_weights_list list must have same length"
        self.choices = list(choices)
        self.choice_indices_by_keyword_map: dict = {}
        LOGGER.debug('keywords_list: %s', keywords_list)
        for choice_index, keywords in enumerate(keywords_list):
            for keyword in _to_list(keywords):
                self.choice_indices_by_keyword_map.setdefault(keyword, []).append(choice_index)
        self.keyword_weights_by_choice_and_keyword_map = {
            choice_index: dict(zip(keywords, keyword_weights))
            for choice_index, (keywords, keyword_weights) in enumerate(zip(
                keywords_list,
                keyword_weights_list
            ))
        }
        self.vectorizer = vectorizer

    @staticmethod
    def from_tf_matrix(
            tf_matrix: np.ndarray,
            tf_keywords: Optional[List[str]] = None,
            vectorizer: Optional[Vectorizer] = None,
            choices: Optional[list] = None,
            **kwargs) -> 'WeightedKeywordModel':
        if choices is None:
            choices = list(range(len(tf_matrix)))
        if tf_keywords is None:
            if vectorizer is not None:
                tf_keywords = vectorizer.get_feature_names()
            else:
                raise ValueError('at least tf_keywords or vectorizer required')
        weighted_keywords_list = get_weighted_keywords_list_for_tf_matrix(
            tf_matrix,
            tf_keywords
        )
        return WeightedKeywordModel(
            choices,
            keywords_list=[
                [keyword for _, keyword in weighted_keywords]
                for weighted_keywords in weighted_keywords_list
            ],
            keyword_weights_list=[
                [keyword_weight for keyword_weight, _ in weighted_keywords]
                for weighted_keywords in weighted_keywords_list
            ],
            vectorizer=vectorizer,
            **kwargs
        )

    def _get_matching_keywords(
            self,
            weighted_keywords: List[Tuple[float, str]]) -> Dict[KT, List[Tuple[float, str]]]:
        if not isinstance(weighted_keywords, list):
            return None
        choice_matching_keywords = defaultdict(list)
        for keyword_weight, keyword in weighted_keywords:
            for choice_index in self.choice_indices_by_keyword_map.get(keyword, []):
                keyword_score = (
                    self.keyword_weights_by_choice_and_keyword_map[choice_index][keyword]
                    * keyword_weight
                )
                choice_matching_keywords[choice_index].append(
                    (keyword_score, keyword)
                )
        return choice_matching_keywords

    def predict_ranking(
            self,
            keywords_list: List[List[str]]) -> WeightedKeywordModelRankingList:
        weighted_keywords_list = get_weighted_keywords_list_for_vectorizer(
            keywords_list=keywords_list,
            vectorizer=self.vectorizer
        )
        return WeightedKeywordModelRankingList(
            [
                self._get_matching_keywords(weighted_keywords)
                for weighted_keywords in weighted_keywords_list
            ],
            choices=self.choices
        )

    def predict(
            self,
            keywords_list: List[List[str]],
            limit: Optional[int] = None,
            return_scores: bool = False):
        LOGGER.debug('keywords_list: %s', keywords_list)
        index = _get_index(keywords_list)
        result = self.predict_ranking(keywords_list)
        if return_scores:
            values = result.get_ranked_scores_list(limit=limit)
        else:
            values = result.get_ranked_choices_list(limit=limit)
        LOGGER.debug('index: %s', index)
        return pd.Series(values, index=index)

    def predict_single(
            self,
            keywords: list,
            debug: bool = False,
            **kwargs):  # pylint: disable=unused-argument
        return self.predict([keywords], **kwargs)[0]
