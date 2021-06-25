import os
import json
import logging
import jsonschema

from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest

from data_science_pipeline.peerscout.api.extract_keywords import (
    get_keyword_extractor
)

from elife_data_hub_utils.keyword_extract.spacy_keyword import (
    DEFAULT_SPACY_LANGUAGE_MODEL_NAME
)

from data_science_pipeline.peerscout.api.recomend_editor import (
    load_model,
    get_editor_recomendations_for_api
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"
DATA_SCIENCE_STATE_PATH_ENV_NAME = "DATA_SCIENCE_STATE_PATH"
DEFAULT_STATE_PATH_FORMAT = (
    "s3://{env}-elife-data-pipeline/airflow-config/data-science/state"
)

SENIOR_EDITOR_MODEL_NAME = 'senior_editor_model.joblib'
REVIEWING_EDITOR_MODEL_NAME = 'reviewing_editor_model.joblib'
DEFAULT_N_FOR_TOP_N_EDITORS = 3

LOGGER = logging.getLogger(__name__)

REQUEST_JSON_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'input-json-schema.json')


def get_deployment_env() -> str:
    return os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV_VALUE
    )


def get_model_path(deployment_env: str) -> str:
    return os.getenv(
        DATA_SCIENCE_STATE_PATH_ENV_NAME,
        DEFAULT_STATE_PATH_FORMAT.format(env=deployment_env)
    )


def get_recommendation_html(person_ids: list, names: list) -> str:
    if not person_ids:
        return '<b>No</b> recommendation available'
    return '<b>The recommended editors are:</b> %s' % names


def get_recommendation_json(person_ids: list, names: list) -> dict:
    return {
       'person_ids': person_ids,
       'recommendation_html': get_recommendation_html(person_ids, names)
    }


def get_response_json(
        senior_editor_person_ids: list,
        senior_editor_names: list,
        reviewing_editor_person_ids: list,
        reviewing_editor_names: list,
        keywords: list) -> dict:
    return {
        'senior_editor_recommendation': get_recommendation_json(
            senior_editor_person_ids,
            senior_editor_names),
        'reviewing_editor_recommendation': get_recommendation_json(
            reviewing_editor_person_ids,
            reviewing_editor_names),
        # added temproraly to see the keywords
        'extracted_keywords': keywords
    }


def create_app():
    app = Flask(__name__)
    keyword_extractor = get_keyword_extractor(DEFAULT_SPACY_LANGUAGE_MODEL_NAME)

    MODEL_PATH = get_model_path(get_deployment_env())
    senior_editor_model_dict = load_model(MODEL_PATH, SENIOR_EDITOR_MODEL_NAME)
    reviewing_editor_model_dict = load_model(MODEL_PATH, REVIEWING_EDITOR_MODEL_NAME)

    with open(REQUEST_JSON_SCHEMA_PATH) as f:
        json_schema = json.load(f)

    @app.route('/', methods=['GET'])
    def _home():
        html = """<h1>PeerScout Recommendation API</h1>
        <p>This site is a prototype PeerScout Recommendation API for
        senior and reviewing editor recommendations.</p>"""
        return html

    @app.route('/api/status', methods=['GET'])
    def _status():
        return jsonify({"status": "OK"})

    @app.route('/api/peerscout', methods=['POST'])
    def _peerscout_api():
        data = request.get_json(force=True)
        LOGGER.info('Processing the request: %s', data)

        try:
            jsonschema.validate(data, json_schema)
        except jsonschema.exceptions.ValidationError as e:
            LOGGER.info('invalid JSON %s, %s', data, e)
            raise BadRequest() from e

        abstract = data['abstract']
        extracted_keywords = list(keyword_extractor.iter_extract_keywords(text_list=[abstract]))

        recomended_senior_editors = get_editor_recomendations_for_api(
            senior_editor_model_dict,
            extracted_keywords,
            DEFAULT_N_FOR_TOP_N_EDITORS
        )

        recomended_reviewing_editors = get_editor_recomendations_for_api(
            reviewing_editor_model_dict,
            extracted_keywords,
            DEFAULT_N_FOR_TOP_N_EDITORS
        )

        suggested_senior_editor_person_ids = recomended_senior_editors['person_id'].to_list()
        suggested_reviewing_editor_person_ids = recomended_reviewing_editors['person_id'].to_list()

        suggested_senior_editor_names = recomended_senior_editors['name'].to_list()
        suggested_reviewing_editor_names = recomended_reviewing_editors['name'].to_list()

        return jsonify(get_response_json(
            senior_editor_person_ids=suggested_senior_editor_person_ids,
            reviewing_editor_person_ids=suggested_reviewing_editor_person_ids,
            senior_editor_names=suggested_senior_editor_names,
            reviewing_editor_names=suggested_reviewing_editor_names,
            # added temproraly to see the keywords
            keywords=extracted_keywords
        ))

    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    main()
