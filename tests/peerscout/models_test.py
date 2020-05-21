import logging

import numpy as np
import pandas as pd

from data_science_pipeline.peerscout.models import (
    WeightedKeywordModel
)


LOGGER = logging.getLogger(__name__)


NAME_1 = 'name 1'
NAME_2 = 'name 2'
NAME_3 = 'name 3'

KEYWORD_1 = 'keyword1'
KEYWORD_2 = 'keyword2'


class TestWeightedKeywordModel:
    def test_should_recommend_person_that_has_matching_keywords(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[1.0], [1.0]]
        )
        result = model.predict([[KEYWORD_1], [KEYWORD_2]])
        LOGGER.debug('result: %s', result)
        assert list(result) == [[NAME_1], [NAME_2]]

    def test_should_preserve_pandas_index(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[1.0], [1.0]]
        )
        result = model.predict(
            pd.Series({
                'A': [KEYWORD_1],
                'B': [KEYWORD_2]
            })
        )
        LOGGER.debug('result: %s', result)
        assert list(result) == [[NAME_1], [NAME_2]]
        assert list(result.index) == ['A', 'B']

    def test_should_recommend_person_that_has_matching_keywords_with_limit(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2, NAME_3],
            [[KEYWORD_1], [KEYWORD_1], [KEYWORD_1]],
            [[0.5], [1.0], [0.1]]
        )
        result = model.predict([[KEYWORD_1]], limit=2)
        LOGGER.debug('result: %s', result)
        assert list(result) == [[NAME_2, NAME_1]]

    def test_should_recommend_person_that_has_matching_keywords_via_predict_single(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[1.0], [1.0]]
        )
        result = model.predict_single([KEYWORD_1])
        LOGGER.debug('result: %s', result)
        assert list(result) == [NAME_1]

    def test_should_recommend_person_that_has_matching_keywords_via_ranking(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[1.0], [1.0]]
        )
        result = model.predict_ranking([[KEYWORD_1], [KEYWORD_2]]).ranked_choices_list
        LOGGER.debug('result: %s', result)
        assert list(result) == [[NAME_1], [NAME_2]]

    def test_should_recommend_person_that_has_matching_keywords_via_ranking_with_limit(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2, NAME_3],
            [[KEYWORD_1], [KEYWORD_1], [KEYWORD_1]],
            [[0.5], [1.0], [0.1]]
        )
        result = model.predict_ranking([[KEYWORD_1]]).get_ranked_choices_list(
            limit=2
        )
        LOGGER.debug('result: %s', result)
        assert result == [[NAME_2, NAME_1]]

    def test_should_sort_matching_keywords_by_score(self):
        model = WeightedKeywordModel(
            [NAME_1],
            [[KEYWORD_1, KEYWORD_2]],
            [[0.1, 0.2]]
        )
        result = model.predict_ranking([[KEYWORD_1, KEYWORD_2]]).matching_keywords_list
        LOGGER.debug('result: %s', result)
        assert list(result) == [[[(0.2, KEYWORD_2), (0.1, KEYWORD_1)]]]

    def test_should_sort_matching_keywords_by_keyword_if_score_is_the_same(self):
        model = WeightedKeywordModel(
            [NAME_1],
            [[KEYWORD_1, KEYWORD_2]],
            [[0.1, 0.1]]
        )
        result = model.predict_ranking([[KEYWORD_1, KEYWORD_2]]).matching_keywords_list
        LOGGER.debug('result: %s', result)
        assert list(result) == [[[(0.1, KEYWORD_1), (0.1, KEYWORD_2)]]]

    def test_should_return_score_with_matching_keywords(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[0.1], [0.2]]
        )
        result = list(model.predict(
            [[KEYWORD_1], [KEYWORD_2]],
            return_scores=True
        ))
        LOGGER.debug('result: %s', result)
        assert list(result) == [[{
            'index': 0,
            'value': NAME_1,
            'score': 0.1,
            'matching_keywords': [(0.1, KEYWORD_1)]
        }], [{
            'index': 1,
            'value': NAME_2,
            'score': 0.2,
            'matching_keywords': [(0.2, KEYWORD_2)]
        }]]

    def test_should_return_score_with_matching_keywords_via_ranking(self):
        model = WeightedKeywordModel(
            [NAME_1, NAME_2],
            [[KEYWORD_1], [KEYWORD_2]],
            [[0.1], [0.2]]
        )
        result = model.predict_ranking(
            [[KEYWORD_1], [KEYWORD_2]]
        )
        LOGGER.debug('result: %s', result)
        assert result.ranked_scores_list == [[{
            'index': 0,
            'value': NAME_1,
            'score': 0.1,
            'matching_keywords': [(0.1, KEYWORD_1)]
        }], [{
            'index': 1,
            'value': NAME_2,
            'score': 0.2,
            'matching_keywords': [(0.2, KEYWORD_2)]
        }]]
        assert result.matching_keywords_list == [
            [
                [(0.1, KEYWORD_1)],
                []
            ],
            [
                [],
                [(0.2, KEYWORD_2)]
            ]
        ]
        assert result.proba_matrix.tolist() == [
            [0.1, 0.0],
            [0.0, 0.2]
        ]

    def test_should_be_able_to_create_from_tf_matrix(self):
        model = WeightedKeywordModel.from_tf_matrix(
            np.asarray([
                [0.1, 0.0],
                [0.0, 0.2]
            ]),
            [KEYWORD_1, KEYWORD_2],
            choices=[NAME_1, NAME_2]
        )
        result = model.predict_ranking(
            [[KEYWORD_1], [KEYWORD_2]]
        )
        LOGGER.debug('result: %s', result)
        assert result.ranked_scores_list == [[{
            'index': 0,
            'value': NAME_1,
            'score': 0.1,
            'matching_keywords': [(0.1, KEYWORD_1)]
        }], [{
            'index': 1,
            'value': NAME_2,
            'score': 0.2,
            'matching_keywords': [(0.2, KEYWORD_2)]
        }]]

    def test_should_be_able_to_create_from_tf_matrix_without_names(self):
        model = WeightedKeywordModel.from_tf_matrix(
            np.asarray([
                [0.1, 0.0],
                [0.0, 0.2]
            ]),
            [KEYWORD_1, KEYWORD_2]
        )
        result = model.predict_ranking(
            [[KEYWORD_1], [KEYWORD_2]]
        )
        LOGGER.debug('result: %s', result)
        assert result.ranked_scores_list == [[{
            'index': 0,
            'value': 0,
            'score': 0.1,
            'matching_keywords': [(0.1, KEYWORD_1)]
        }], [{
            'index': 1,
            'value': 1,
            'score': 0.2,
            'matching_keywords': [(0.2, KEYWORD_2)]
        }]]
