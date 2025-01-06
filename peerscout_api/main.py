from datetime import datetime, timezone
import os
import json
import logging
import html
from typing import NamedTuple, Optional
from threading import Thread
import jsonschema

from google.cloud.bigquery import Client
from google.cloud import bigquery
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    get_keyword_extractor as get_keyword_extractor_for_spacy_language_model,
    KeywordExtractor
)

from data_science_pipeline.utils.json import remove_key_with_null_value
from data_science_pipeline.utils.bq import load_json_list_and_append_to_bq_table_with_auto_schema

from peerscout_api.recommend_editor import (
    load_model,
    get_editor_recommendations_for_api
)

DEFAULT_SPACY_LANGUAGE_MODEL_NAME = "en_core_web_sm"
SPACY_LANGUAGE_MODEL_NAME_ENV_VALUE = "SPACY_LANGUAGE_MODEL_NAME"
SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE = "SPACY_KEYWORD_EXTRACTION_API_URL"

PEERSCOUT_API_TARGET_DATASET_ENV_NAME = "PEERSCOUT_API_TARGET_DATASET"
DEFAULT_PEERSCOUT_API_TARGET_DATASET_VALUE = "ci"

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"
DATA_SCIENCE_STATE_PATH_ENV_NAME = "DATA_SCIENCE_STATE_PATH"
DEFAULT_STATE_PATH_FORMAT = (
    "s3://{env}-elife-data-pipeline/airflow-config/data-science/state"
)
REQUEST_JSON_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'input-json-schema.json')

TARGET_TABLE_NAME = 'peerscout_api_reviewer_recommendations'

SENIOR_EDITOR_MODEL_NAME = 'senior_editor_model.joblib'
REVIEWING_EDITOR_MODEL_NAME = 'reviewing_editor_model.joblib'
DEFAULT_N_FOR_TOP_N_EDITORS = 3

NO_RECOMMENDATION_TEXT = ' No recommendation available'
NOT_PROVIDED = 'Not provided'

EDITOR_TYPE_FOR_SENIOR_EDITOR = 'Senior'
EDITOR_TYPE_FOR_REVIEWING_EDITOR = 'Reviewing'

HEADING_STYLE = (
    "font-family:sans-serif; color:black; margin-bottom:5px; font-size:1.02em; font-weight:bold;"
)

RECOMMENDATION_HEADINGS = [
    (
        '<h4 style="margin-top: 20px;'
        + HEADING_STYLE
        + '">Author Requested {editor_type} Editor Exclusions:</h4>'),
    (
        '<h4 style="margin-top: 15px;'
        + HEADING_STYLE
        + '">Author Suggested {editor_type} Editors:</h4>'),
    (
        '<h4 style="margin-top: 15px;'
        + HEADING_STYLE
        + '">Recommended {editor_type} Editors (based on keyword matching):</h4>')]

RECOMMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + '{excluded_editor_details}' + \
    RECOMMENDATION_HEADINGS[1] + '{included_editor_details}' + \
    RECOMMENDATION_HEADINGS[2] + '{recommended_editor_details}'

NO_RECOMMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[1] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[2] + NO_RECOMMENDATION_TEXT

QUERY_PATH = os.path.join(os.path.dirname(__file__), 'person_details_and_stats.sql')

LOGGER = logging.getLogger(__name__)


class PersonProps(NamedTuple):
    person_name: str
    institution: Optional[str] = None
    country: Optional[str] = None
    availability: Optional[str] = None
    website: Optional[str] = None
    pubmed: Optional[str] = None
    days_to_respond: Optional[str] = None
    requests: Optional[str] = None
    responses: Optional[str] = None
    response_rate: Optional[str] = None
    no_of_assigments: Optional[str] = None
    no_of_full_submissions: Optional[str] = None
    decision_time: Optional[str] = None


def get_current_timestamp_as_string(
        time_format: str = "%Y-%m-%dT%H:%M:%SZ"
):
    dtobj = datetime.now(timezone.utc)
    return dtobj.strftime(time_format)


def get_deployment_env() -> str:
    return os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV_VALUE
    )


def get_target_dataset_env() -> str:
    return os.getenv(
        PEERSCOUT_API_TARGET_DATASET_ENV_NAME,
        DEFAULT_PEERSCOUT_API_TARGET_DATASET_VALUE
    )


def get_spacy_language_model_env() -> str:
    return os.getenv(
        SPACY_LANGUAGE_MODEL_NAME_ENV_VALUE,
        DEFAULT_SPACY_LANGUAGE_MODEL_NAME
    )


def get_spacy_keyword_extraction_api_url() -> Optional[str]:
    return os.getenv(
        SPACY_KEYWORD_EXTRACTION_API_URL_ENV_VALUE
    )


class SpaCyApiKeywordExtractor():
    pass


def get_keyword_extractor() -> KeywordExtractor:
    api_url = get_spacy_keyword_extraction_api_url()
    if api_url:
        return SpaCyApiKeywordExtractor()
    return get_keyword_extractor_for_spacy_language_model(get_spacy_language_model_env())


def get_model_path(deployment_env: str) -> str:
    return os.getenv(
        DATA_SCIENCE_STATE_PATH_ENV_NAME,
        DEFAULT_STATE_PATH_FORMAT.format(env=deployment_env)
    )


def query_bq_for_person_details(
        project: str,
        dataset: str,
        person_ids: list):

    client = Client(project=project)

    with open(QUERY_PATH, encoding='utf-8') as f:
        sql_file = f.read()

    sql = (
        sql_file.format(
            project=project,
            dataset=dataset
        )
    )

    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("person_ids", "STRING", person_ids)
    ])

    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
    return list(results)


def get_person_details_from_bq(
    person_ids: list
):
    project_name = 'elife-data-pipeline'
    dataset_name = get_deployment_env()

    return query_bq_for_person_details(
        project=project_name,
        dataset=dataset_name,
        person_ids=person_ids
    )


def get_html_text_for_recommended_person(
    person: PersonProps
) -> str:
    return (
        person.person_name
        + ('<br />' if person.institution else '')
        + (person.institution if person.institution else '')
        + ((', ' + person.country) if (person.country and person.institution) else '')
        + (('<br /><span style=\'color:red;\'><strong>!</strong></span> Limited availability: '
            + person.availability) if person.availability else '')
        + ('<br />' if (person.website or person.pubmed) else '')
        + (('<a href="' + html.escape(person.website) + '" target=_blank>Website</a>')
            if person.website else '')
        + (' | ' if (person.website and person.pubmed) else '')
        + (('<a href="' + html.escape(person.pubmed) + '" target=_blank>PubMed</a>')
            if person.pubmed else '')
        + ('<br />' if (
            person.days_to_respond
            or person.requests
            or person.responses
            or person.response_rate
        ) else '')
        + (('Days to respond: ' + person.days_to_respond)
            if person.days_to_respond else '')
        + ('; ' if (
            person.days_to_respond
            and person.requests
            ) else '')
        + (('Requests: ' + person.requests) if person.requests else '')
        + ('; ' if (
            (
                person.days_to_respond
                or person.requests)
            and person.responses
            ) else '')
        + (('Responses: ' + person.responses) if person.responses else '')
        + ('; ' if (
            (
                person.days_to_respond
                or person.requests
                or person.responses)
            and person.response_rate
            ) else '')
        + (('Response rate: ' + person.response_rate + '%') if person.response_rate else '')
        + ('<br />' if (
            person.no_of_assigments
            or person.no_of_full_submissions
            or person.decision_time
        ) else '')
        + (('No. of current assignments: ' + person.no_of_assigments)
            if person.no_of_assigments else '')
        + ('; ' if (
            person.no_of_assigments
            and person.no_of_full_submissions
            ) else '')
        + (('Full submissions in 12 months: ' + person.no_of_full_submissions)
            if person.no_of_full_submissions else '')
        + ('; ' if (
            (
                person.no_of_assigments
                or person.no_of_full_submissions)
            and person.decision_time
            ) else '')
        + (('Decision time: ' + person.decision_time + ' days')
            if person.decision_time else '')
    )


def get_html_text_for_author_suggested_person(
    person: PersonProps
):
    if not person.institution:
        return f'{person.person_name}'

    return f'{person.person_name}; {person.institution}'


def get_list_of_recommended_person_details_with_html_text(
    result_of_person_details_from_bq
) -> str:
    person_details = (
        [
            get_html_text_for_recommended_person(person)
            for person in result_of_person_details_from_bq
        ]
    )
    return '<br /><br />'.join(person_details)


def get_list_of_author_suggested_person_details_with_html_text(
    result_of_person_details_from_bq
):
    person_details = (
        [
            get_html_text_for_author_suggested_person(person)
            for person in result_of_person_details_from_bq
        ]
    )
    if person_details:
        return '<br />'.join(person_details)
    else:
        return NOT_PROVIDED


def pick_person_id_from_bq_result(
    bq_person_detail_sql_result,
    person_ids_to_pick: list
) -> list:
    picked_person_details = []
    # the complexity will not be an issue as person_ids_to_pick list will contain up to 3 ids
    for person_id in person_ids_to_pick:
        for result in bq_person_detail_sql_result:
            if person_id == result['person_id']:
                picked_person_details.append(result)

    return picked_person_details


def add_html_formated_person_details_to_recommendation_html(
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
        recommended_person_ids: list,
        editor_type: str
) -> str:

    all_editor_ids = (
        author_suggestion_exclude_editor_ids
        + author_suggestion_include_editor_ids
        + recommended_person_ids
    )

    result_person_details = get_person_details_from_bq(
        person_ids=all_editor_ids
    )

    formated_suggested_exclude_editor_details = \
        get_list_of_author_suggested_person_details_with_html_text(
            pick_person_id_from_bq_result(
                result_person_details,
                author_suggestion_exclude_editor_ids
            )
        )
    formated_suggested_include_editor_details = \
        get_list_of_author_suggested_person_details_with_html_text(
            pick_person_id_from_bq_result(
                result_person_details,
                author_suggestion_include_editor_ids
            )
        )
    formated_recommended_editor_details = \
        get_list_of_recommended_person_details_with_html_text(
            pick_person_id_from_bq_result(
                result_person_details,
                recommended_person_ids
            )
        )

    return RECOMMENDATION_HTML.format(
        excluded_editor_details=formated_suggested_exclude_editor_details,
        included_editor_details=formated_suggested_include_editor_details,
        recommended_editor_details=formated_recommended_editor_details,
        editor_type=editor_type)


def get_recommendation_html(
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
        recommended_person_ids: list,
        editor_type: str
) -> str:
    if not recommended_person_ids:
        return NO_RECOMMENDATION_HTML.format(editor_type=editor_type)

    return add_html_formated_person_details_to_recommendation_html(
        author_suggestion_exclude_editor_ids=author_suggestion_exclude_editor_ids,
        author_suggestion_include_editor_ids=author_suggestion_include_editor_ids,
        recommended_person_ids=recommended_person_ids,
        editor_type=editor_type
    )


def get_recommendation_json(
        recommended_person_ids: list,
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
        editor_type: str
) -> dict:
    return {
       'person_ids': recommended_person_ids,
       'recommendation_html': get_recommendation_html(
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_editor_ids,
            author_suggestion_include_editor_ids=author_suggestion_include_editor_ids,
            recommended_person_ids=recommended_person_ids,
            editor_type=editor_type
        )
    }


def get_response_json(
        senior_editor_recommendation_json: dict,
        reviewing_editor_recommendation_json: dict
) -> dict:
    return {
        'senior_editor_recommendation': senior_editor_recommendation_json,
        'reviewing_editor_recommendation': reviewing_editor_recommendation_json
        }


def write_peerscout_api_response_to_bq(
    recommendation_request: dict,
    recommendation_response: dict
):
    project_name = 'elife-data-pipeline'
    target_dataset_name = get_target_dataset_env()

    recommendation_response_with_provenance = remove_key_with_null_value({
        **recommendation_response,
        'provenance': {
            'recommendation_request': {**recommendation_request},
            'imported_timestamp': get_current_timestamp_as_string()
        }
    })
    LOGGER.info(
        "API Recommendation JSON for BQ load: %r",
        recommendation_response_with_provenance
    )

    load_json_list_and_append_to_bq_table_with_auto_schema(
        project_id=project_name,
        dataset_name=target_dataset_name,
        table_name=TARGET_TABLE_NAME,
        json_list=[recommendation_response_with_provenance],
    )


def write_peerscout_api_response_to_bq_in_a_thread(
    recommendation_request: dict,
    recommendation_response: dict
):
    thread = Thread(
        target=write_peerscout_api_response_to_bq,
        args=(recommendation_request, recommendation_response)
    )
    thread.start()


def create_app():
    app = Flask(__name__)
    keyword_extractor = get_keyword_extractor()

    MODEL_PATH = get_model_path(get_deployment_env())
    senior_editor_model_dict = load_model(MODEL_PATH, SENIOR_EDITOR_MODEL_NAME)
    reviewing_editor_model_dict = load_model(MODEL_PATH, REVIEWING_EDITOR_MODEL_NAME)

    with open(REQUEST_JSON_SCHEMA_PATH, encoding='utf-8') as f:
        json_schema = json.load(f)

    @app.route('/', methods=['GET'])
    def _home():
        html_result = """<h1>PeerScout Recommendation API</h1>
        <p>PeerScout Recommendation API for
        senior and reviewing editor recommendations.</p>"""
        return html_result

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
        author_suggestion_exclude_senior_editors = author_suggestion[
            'exclude_senior_editors_id'
        ]
        author_suggestion_include_senior_editors = author_suggestion[
            'include_senior_editors_id'
        ]
        author_suggestion_exclude_reviewing_editors = author_suggestion[
            'exclude_reviewing_editors_id'
        ]
        author_suggestion_include_reviewing_editors = author_suggestion[
            'include_reviewing_editors_id'
        ]

        if not abstract:
            recommeded_senior_editor_ids = []
            recommeded_reviewing_editor_ids = []
        else:
            extracted_keywords = list(keyword_extractor.iter_extract_keywords(text_list=[abstract]))

            recommended_senior_editors = get_editor_recommendations_for_api(
                senior_editor_model_dict,
                extracted_keywords,
                DEFAULT_N_FOR_TOP_N_EDITORS
            )
            recommended_reviewing_editors = get_editor_recommendations_for_api(
                reviewing_editor_model_dict,
                extracted_keywords,
                DEFAULT_N_FOR_TOP_N_EDITORS
            )

            recommeded_senior_editor_ids = recommended_senior_editors['person_id'].to_list()
            recommeded_reviewing_editor_ids = recommended_reviewing_editors['person_id'].to_list()

        json_response_for_senior_editors = get_recommendation_json(
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_senior_editors,
            author_suggestion_include_editor_ids=author_suggestion_include_senior_editors,
            recommended_person_ids=recommeded_senior_editor_ids,
            editor_type=EDITOR_TYPE_FOR_SENIOR_EDITOR
        )

        json_response_for_reviewing_editors = get_recommendation_json(
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_reviewing_editors,
            author_suggestion_include_editor_ids=author_suggestion_include_reviewing_editors,
            recommended_person_ids=recommeded_reviewing_editor_ids,
            editor_type=EDITOR_TYPE_FOR_REVIEWING_EDITOR
        )

        api_json_response = get_response_json(
            senior_editor_recommendation_json=json_response_for_senior_editors,
            reviewing_editor_recommendation_json=json_response_for_reviewing_editors
        )

        write_peerscout_api_response_to_bq_in_a_thread(
            recommendation_request=data,
            recommendation_response=api_json_response
        )

        return jsonify(api_json_response)

    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    main()
