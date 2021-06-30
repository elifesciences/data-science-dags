import os
import json
import logging
import jsonschema

from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest

from elife_data_hub_utils.keyword_extract.spacy_keyword import (
    DEFAULT_SPACY_LANGUAGE_MODEL_NAME
)

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    get_keyword_extractor
)

from peerscout_api.recomend_editor import (
    load_model,
    get_editor_recomendations_for_api
)

from google.cloud.bigquery import Client


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

NO_RECOMENDATION_TEXT = ' No recommendation available'
NOT_PROVIDED = 'Not provided'
RECOMMENDATION_HEADINGS = [
    '<p><strong>Author’s requests for Editor exclusions:</strong></p>',
    '<p><strong>Author’s suggestions for Reviewing Editor:</strong></p>',
    '<p><strong>Recommended Editors (based on keyword matching):</strong></p>']

NO_RECOMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[1] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[2] + NO_RECOMENDATION_TEXT

RECOMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + '{formated_excluded_name}' + \
    RECOMMENDATION_HEADINGS[1] + '{formated_included_name}' + \
    RECOMMENDATION_HEADINGS[2] + '{formated_recomended_name}'


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


def get_person_names_from_bq(
        project: str,
        dataset: str,
        table: str,
        person_ids: list):

    client = Client(project=project)

    sql = (
        """
        SELECT
        IF(
            middle_name IS NOT NULL,
            CONCAT(first_name,' ',middle_name,' ',last_name),
            CONCAT(first_name,' ',last_name)) AS person_name
        FROM `{project}.{dataset}.{table}`
        WHERE person_id IN UNNEST({person_ids})
        """.format(
            project=project,
            dataset=dataset,
            table=table,
            person_ids=person_ids
        )
    )

    query_job = client.query(sql)
    results = query_job.result()
    return [row.person_name for row in results]


def get_formated_name_for_html(names: list) -> str:
    formated_name = ''
    for name in names:
        formated_name += "<br />" + name
    return formated_name


def get_formated_html_text(
        recommended_names: list,
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
) -> str:
    PROJECT_NAME = 'elife-data-pipeline'
    DATASET_NAME = get_deployment_env()

    formated_recomended_name = get_formated_name_for_html(
            recommended_names
        )
    author_suggestion_include_editor_names = get_person_names_from_bq(
            project=PROJECT_NAME,
            dataset=DATASET_NAME,
            table='mv_person',
            person_ids=author_suggestion_include_editor_ids
        )

    author_suggestion_exclude_editor_names = get_person_names_from_bq(
        project=PROJECT_NAME,
        dataset=DATASET_NAME,
        table='mv_person',
        person_ids=author_suggestion_exclude_editor_ids
    )
    formated_excluded_name = get_formated_name_for_html(
        author_suggestion_exclude_editor_names
    )
    formated_included_name = get_formated_name_for_html(
        author_suggestion_include_editor_names
    )
    return RECOMENDATION_HTML.format(
        formated_excluded_name=formated_excluded_name,
        formated_included_name=formated_included_name,
        formated_recomended_name=formated_recomended_name)


def get_recommendation_html(
        editor_type: str,
        recommended_person_ids: list,
        recommended_names: list,
        author_suggestion_exclude_senior_editor_ids: list,
        author_suggestion_include_senior_editor_ids: list,
        author_suggestion_exclude_reviewing_editor_ids: list,
        author_suggestion_include_reviewing_editor_ids: list
) -> str:
    if not recommended_person_ids:
        return NO_RECOMENDATION_HTML

    if editor_type == 'senior':
        return get_formated_html_text(
            recommended_names=recommended_names,
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_senior_editor_ids,
            author_suggestion_include_editor_ids=author_suggestion_include_senior_editor_ids
        )
    else:
        return get_formated_html_text(
            recommended_names=recommended_names,
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_reviewing_editor_ids,
            author_suggestion_include_editor_ids=author_suggestion_include_reviewing_editor_ids
        )


def get_recommendation_json(
        editor_type: str,
        recommended_person_ids: list,
        recommended_names: list,
        author_suggestion_exclude_senior_editor_ids: list,
        author_suggestion_include_senior_editor_ids: list,
        author_suggestion_exclude_reviewing_editor_ids: list,
        author_suggestion_include_reviewing_editor_ids: list
) -> dict:
    return {
       'person_ids': recommended_person_ids,
       'recommendation_html': get_recommendation_html(
            editor_type,
            recommended_person_ids,
            recommended_names,
            author_suggestion_exclude_senior_editor_ids,
            author_suggestion_include_senior_editor_ids,
            author_suggestion_exclude_reviewing_editor_ids,
            author_suggestion_include_reviewing_editor_ids
        )
    }


def get_response_json(
        senior_editor_person_ids: list,
        senior_editor_names: list,
        reviewing_editor_person_ids: list,
        reviewing_editor_names: list,
        author_suggestion_exclude_senior_editor_ids: list,
        author_suggestion_include_senior_editor_ids: list,
        author_suggestion_exclude_reviewing_editor_ids: list,
        author_suggestion_include_reviewing_editor_ids: list,
) -> dict:
    return {
        'senior_editor_recommendation': get_recommendation_json(
            editor_type='senior',
            recommended_person_ids=senior_editor_person_ids,
            recommended_names=senior_editor_names,
            author_suggestion_exclude_senior_editor_ids=author_suggestion_exclude_senior_editor_ids,
            author_suggestion_include_senior_editor_ids=author_suggestion_include_senior_editor_ids,
            author_suggestion_exclude_reviewing_editor_ids=[],
            author_suggestion_include_reviewing_editor_ids=[]),
        'reviewing_editor_recommendation': get_recommendation_json(
            editor_type='reviewing',
            recommended_person_ids=reviewing_editor_person_ids,
            recommended_names=reviewing_editor_names,
            author_suggestion_exclude_senior_editor_ids=[],
            author_suggestion_include_senior_editor_ids=[],
            author_suggestion_exclude_reviewing_editor_ids=author_suggestion_exclude_reviewing_editor_ids,
            author_suggestion_include_reviewing_editor_ids=author_suggestion_include_reviewing_editor_ids,
            )
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
        <p>PeerScout Recommendation API for
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

        author_suggestion = data['author_suggestion']
        author_suggestion_exclude_senior_editors = author_suggestion['exclude_senior_editors_id']
        author_suggestion_include_senior_editors = author_suggestion['include_senior_editors_id']
        author_suggestion_exclude_reviewing_editors = author_suggestion['exclude_reviewing_editors_id']
        author_suggestion_include_reviewing_editors = author_suggestion['include_reviewing_editors_id']

        if not abstract:
            suggested_senior_editor_person_ids = []
            suggested_reviewing_editor_person_ids = []
            suggested_senior_editor_names = []
            suggested_reviewing_editor_names = []
        else:
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
            senior_editor_names=suggested_senior_editor_names,
            reviewing_editor_person_ids=suggested_reviewing_editor_person_ids,
            reviewing_editor_names=suggested_reviewing_editor_names,
            author_suggestion_exclude_senior_editor_ids=author_suggestion_exclude_senior_editors,
            author_suggestion_include_senior_editor_ids=author_suggestion_include_senior_editors,
            author_suggestion_exclude_reviewing_editor_ids=author_suggestion_exclude_reviewing_editors,
            author_suggestion_include_reviewing_editor_ids=author_suggestion_include_reviewing_editors
        ))

    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    main()
