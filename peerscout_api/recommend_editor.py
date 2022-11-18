import os
import logging
from typing import List, NamedTuple, Tuple, TypeVar
import pandas as pd

from scipy.sparse import csc_matrix
from sklearn.metrics.pairwise import cosine_similarity

from data_science_pipeline.utils.io import load_object_from
from data_science_pipeline.peerscout.models import (
    Vectorizer,
    WeightedKeywordModel
)

LOGGER = logging.getLogger(__name__)
T = TypeVar('T')


class PeerScoutModelProps(NamedTuple):
    editor_names: List[str]
    editor_person_ids: List[str]
    editor_tf_idf_vectorizer: Vectorizer
    editor_tf_idf: csc_matrix

    def get_editor_person_id_by_name_map(self):
        editor_names = self.editor_names
        editor_person_ids = self.editor_person_ids
        editor_person_id_by_name_map = dict(zip(editor_names, editor_person_ids))
        return editor_person_id_by_name_map


def load_model(model_path: str, model_name: str) -> PeerScoutModelProps:
    model_full_path = os.path.join(model_path, model_name)
    LOGGER.info('Loading model : %s', model_full_path)
    model_dict = load_object_from(model_full_path)

    return PeerScoutModelProps(
        editor_names=model_dict['editor_names'],
        editor_person_ids=model_dict['editor_person_ids'],
        editor_tf_idf_vectorizer=model_dict['editor_tf_idf_vectorizer'],
        editor_tf_idf=model_dict['editor_tf_idf'],
        )


def get_weighted_keyword_valid_model(model: PeerScoutModelProps):
    weighted_keyword_valid_model = WeightedKeywordModel.from_tf_matrix(
        tf_matrix=model.editor_tf_idf.todense(),
        vectorizer=model.editor_tf_idf_vectorizer,
        choices=model.editor_names
    )
    return weighted_keyword_valid_model


def get_keyword_similarity(model: PeerScoutModelProps, extracted_keywords: List[List[str]]):
    keyword_similarity = cosine_similarity(
        model.editor_tf_idf_vectorizer.transform(extracted_keywords),
        model.editor_tf_idf
    )
    return keyword_similarity


def get_manuscript_matching_keywords_list(model: PeerScoutModelProps, extracted_keywords: list):
    weighted_keyword_valid_model = \
        get_weighted_keyword_valid_model(model).predict_ranking(extracted_keywords)
    matching_keywords_list = weighted_keyword_valid_model.matching_keywords_list
    return matching_keywords_list


def get_recommended_editors_with_probability(
        proba_matrix: List[List[float]],
        editors_matching_keywords_list: List[List[List[Tuple[float, str]]]],
        indices: List[T],
        threshold: float = 0.5) -> List[List[Tuple[float, T, float, List[Tuple[float, str]]]]]:
    return [
        sorted([
            (
                p,
                key,
                sum(
                    s for s, _ in editor_matching_keywords
                ),
                editor_matching_keywords
            )
            for p, key, editor_matching_keywords in zip(
                row,
                indices,
                editors_matching_keywords
            ) if p >= threshold
        ], reverse=True)
        for row, editors_matching_keywords in zip(proba_matrix, editors_matching_keywords_list)
    ]


def get_editor_recommendations_for_api(
        model: PeerScoutModelProps,
        extracted_keywords: list,
        top_n_editor: int):

    keyword_similarity = get_keyword_similarity(
        model,
        extracted_keywords
    )
    manuscript_matching_keywords_list = get_manuscript_matching_keywords_list(
        model,
        extracted_keywords
    )
    editor_person_id_by_name_map = model.get_editor_person_id_by_name_map()

    prediction_results_with_similarity = get_recommended_editors_with_probability(
            keyword_similarity,
            manuscript_matching_keywords_list,
            model.editor_names,
            threshold=0.001
        )

    LOGGER.info('Length of prediction result: %s', len(prediction_results_with_similarity))
    LOGGER.info('Count of keywords extracted: %s', len(extracted_keywords))

    prediction_results_flat_df = pd.DataFrame([
        {
            'score': predicted_editor[0],
            'name': predicted_editor[1],
            'person_id': editor_person_id_by_name_map[predicted_editor[1]],
        }
        for row in prediction_results_with_similarity
        for predicted_editor in row
    ])

    return prediction_results_flat_df.head(top_n_editor)
