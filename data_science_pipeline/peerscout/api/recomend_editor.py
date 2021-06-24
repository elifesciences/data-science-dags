import os
import logging
from typing import List, Tuple, T
import pandas as pd

from sklearn.metrics.pairwise import cosine_similarity

from data_science_pipeline.utils.io import load_object_from
from data_science_pipeline.peerscout.models import (
    WeightedKeywordModel
)

LOGGER = logging.getLogger(__name__)


def load_model(model_path: str, model_name: str) -> dict:
    model_full_path = os.path.join(model_path, model_name)
    LOGGER.info('Loading model : %s', model_full_path)
    model_dict = load_object_from(model_full_path)

    return model_dict


def get_editor_tf_idf_vectorizer(model_dict: dict):
    return model_dict['editor_tf_idf_vectorizer']


def get_editor_tf_idf(model_dict: dict):
    return model_dict['editor_tf_idf']


def get_editor_names(model_dict: dict):
    return model_dict['editor_names']


def get_editor_person_id_by_name_map(model_dict: dict):
    editor_names = model_dict['editor_names']
    editor_person_ids = model_dict['editor_person_ids']
    editor_person_id_by_name_map = dict(zip(editor_names, editor_person_ids))
    return editor_person_id_by_name_map


def get_weighted_keyword_valid_model(model_dict: dict):
    editor_tf_idf = get_editor_tf_idf(model_dict)
    editor_tf_idf_vectorizer = get_editor_tf_idf_vectorizer(model_dict)
    editor_names = get_editor_names(model_dict)

    weighted_keyword_valid_model = WeightedKeywordModel.from_tf_matrix(
        editor_tf_idf.todense(),
        vectorizer=editor_tf_idf_vectorizer,
        choices=editor_names
    )
    return weighted_keyword_valid_model


def get_keyword_similarity(model_dict: dict, extracted_keywords: list):
    editor_tf_idf = get_editor_tf_idf(model_dict)
    editor_tf_idf_vectorizer = get_editor_tf_idf_vectorizer(model_dict)

    keyword_similarity = cosine_similarity(
        editor_tf_idf_vectorizer.transform(extracted_keywords),
        editor_tf_idf
    )

    return keyword_similarity


def get_manuscript_matching_keywords_list(model_dict: dict, extracted_keywords: list):
    weighted_keyword_valid_model = \
        get_weighted_keyword_valid_model(model_dict).predict_ranking(extracted_keywords)
    matching_keywords_list = weighted_keyword_valid_model.matching_keywords_list
    return matching_keywords_list


def get_recommended_editors_with_probability(
        proba_matrix: List[List[float]],
        editors_matching_keywords_list: List[List[Tuple[float, str]]],
        indices: List[T],
        threshold: float = 0.5) -> List[List[Tuple[float, T]]]:
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


def get_editor_recomendations_for_api(
        model_dict: dict,
        extracted_keywords: list,
        top_n_editor: int):

    keyword_similarity = get_keyword_similarity(
        model_dict,
        extracted_keywords
    )
    manuscript_matching_keywords_list = get_manuscript_matching_keywords_list(
        model_dict,
        extracted_keywords
    )
    editor_names = get_editor_names(model_dict)
    editor_person_id_by_name_map = get_editor_person_id_by_name_map(model_dict)

    prediction_results_with_similarity = get_recommended_editors_with_probability(
            keyword_similarity,
            manuscript_matching_keywords_list,
            editor_names,
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

    return prediction_results_flat_df.sort_values(
        'score', ascending=False).head(top_n_editor)
