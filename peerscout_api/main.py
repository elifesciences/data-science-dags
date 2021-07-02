import os
import json
import logging
import jsonschema

from google.cloud.bigquery import Client
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest

from elife_data_hub_utils.keyword_extract.spacy_keyword import (
    DEFAULT_SPACY_LANGUAGE_MODEL_NAME
)

from elife_data_hub_utils.keyword_extract.extract_keywords import (
    get_keyword_extractor
)

from peerscout_api.recommend_editor import (
    load_model,
    get_editor_recommendations_for_api
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

NO_RECOMMENDATION_TEXT = ' No recommendation available'
NOT_PROVIDED = 'Not provided'
RECOMMENDATION_HEADINGS = [
    '<p><strong>Author’s requests for Editor exclusions:</strong></p>',
    '<p><strong>Author’s suggestions for Reviewing Editor:</strong></p>',
    '<p><strong>Recommended Editors (based on keyword matching):</strong></p>']

NO_RECOMMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[1] + NOT_PROVIDED + \
    RECOMMENDATION_HEADINGS[2] + NO_RECOMMENDATION_TEXT

RECOMMENDATION_HTML = RECOMMENDATION_HEADINGS[0] + '{excluded_editor_details}' + \
    RECOMMENDATION_HEADINGS[1] + '{included_editor_details}' + \
    RECOMMENDATION_HEADINGS[2] + '{recommended_editor_details}'

QUERY = """
    SELECT 
        IF(
            person.middle_name IS NOT NULL,
            CONCAT(person.first_name,' ',person.middle_name,' ',person.last_name),
            CONCAT(person.first_name,' ',person.last_name) 
        ) AS person_name,
        person.institution,
        address.country,
        profile.Website_URL,
        profile.PubMed_URL,
        event.*
    FROM `{project}.{dataset}.mv_person` AS person,
    UNNEST(person.addresses) AS address
    INNER JOIN `{project}.{dataset}.mv_Editorial_Editor_Profile` AS profile
    ON person.person_id = profile.Person_ID
    LEFT JOIN 
    (SELECT 
        DISTINCT 
        Person.Person_ID AS person_id,
        CAST(ROUND(PERCENTILE_CONT(Initial_Submission.Reviewing_Editor.Consultation.Days_To_Respond, 0.5
            ) OVER (PARTITION BY Person.Person_ID),2) AS STRING) AS days_to_respond,
        CAST(COUNT(DISTINCT Initial_Submission.Reviewing_Editor.Consultation.Request_Version_ID
            ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS requests,
        CAST(COUNT(DISTINCT Initial_Submission.Reviewing_Editor.Consultation.Response_Version_ID
            ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS responses,
        CAST(CAST(ROUND(AVG(Initial_Submission.Reviewing_Editor.Consultation.Has_Response_Ratio
            ) OVER (PARTITION BY Person.Person_ID)*100,0) AS INT64) AS STRING) AS response_rate,
        CAST(MAX(Full_Submission.Reviewing_Editor.Current_Assignment_Count
            ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS no_of_assigments,
        CAST(COUNT(DISTINCT Full_Submission.Reviewing_Editor.Assigned_Version_ID
            ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS no_full_submissions,
        CAST(PERCENTILE_CONT(Full_Submission.Reviewing_Editor.Submission_Received_To_Decision_Complete, 0.5
            ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS decision_time, 
        FROM 
        `{project}.{dataset}.mv_Editorial_Editor_Workload_Event`,
        UNNEST(Person.Roles) AS person_role
        WHERE DATE(Event_Timestamp)BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
        AND person_role.Role_Name='Editorial Board Member'
        AND Person.Person_ID IN UNNEST({person_ids})
    ) AS event
    ON person.person_id = event.person_id
    WHERE person.person_id IN UNNEST({person_ids})
"""


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


def get_person_details_from_bq(
        project: str,
        dataset: str,
        person_ids: list):

    client = Client(project=project)

    sql = (
        QUERY.format(
            project=project,
            dataset=dataset,
            person_ids=person_ids
        )
    )

    query_job = client.query(sql)
    results = query_job.result()
    return [row for row in results]


def format_details_for_html(details: list) -> str:
    formated_detail = ''
    for detail in details:
        formated_detail += "<br />" + detail
    return formated_detail


def get_formated_person_details_for_html(
    person_ids: list,
    is_person_recommended: bool
) -> str:
    PROJECT_NAME = 'elife-data-pipeline'
    DATASET_NAME = get_deployment_env()

    result_person_details = get_person_details_from_bq(
        project=PROJECT_NAME,
        dataset=DATASET_NAME,
        person_ids=person_ids
    )
# Days to respond: 1.1; Requests: 15; Responses: 15; Response rate: 100%
# No. of current assignments: 0; Full submissions in 12 months: 1; Decision time: 55 days.
    if is_person_recommended:
        person_details = (
            [
                '<p>'
                + person.person_name + '<br />' + person.institution + ', ' + person.country
                + '<br /><a href=' + person.Website_URL + '>Website</a> | <a href='
                + person.PubMed_URL + '>PubMed</a>'
                # stats for initial submission
                + ('<br />Days to respond: ' + person.days_to_respond if person.days_to_respond else '')
                + ('; Requests: ' + person.requests if person.requests else '')
                + ('; Responses: ' + person.responses if person.responses else '')
                + ('; Response rate: ' + person.response_rate + '%' if person.response_rate else '')
                # stats for full submission
                + ('<br />No. of current assignments: ' + person.no_of_assigments if person.no_of_assigments else '')
                + ('; Full submissions in 12 months: ' + person.no_full_submissions if person.no_full_submissions else '')
                + ('; Decision time: ' + person.decision_time + ' days' if person.decision_time else '')
                + '</p>'
                for person in result_person_details
            ]
        )
    else:
        person_details = (
            [
                person.person_name + '; ' + person.institution + ', ' + person.country
                for person in result_person_details
            ]
        )

    return format_details_for_html(details=person_details)


def get_formated_html_text(
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
        recommended_person_ids: list,
) -> str:

    formated_suggested_exclude_editor_details = get_formated_person_details_for_html(
        person_ids=author_suggestion_exclude_editor_ids,
        is_person_recommended=False
    )
    formated_suggested_include_editor_details = get_formated_person_details_for_html(
        person_ids=author_suggestion_include_editor_ids,
        is_person_recommended=False
    )
    formated_recommended_editor_details = get_formated_person_details_for_html(
        person_ids=recommended_person_ids,
        is_person_recommended=True
    )

    return RECOMMENDATION_HTML.format(
        excluded_editor_details=formated_suggested_exclude_editor_details,
        included_editor_details=formated_suggested_include_editor_details,
        recommended_editor_details=formated_recommended_editor_details)


def get_recommendation_html(
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list,
        recommended_person_ids: list
) -> str:
    if not recommended_person_ids:
        return NO_RECOMMENDATION_HTML

    return get_formated_html_text(
        author_suggestion_exclude_editor_ids=author_suggestion_exclude_editor_ids,
        author_suggestion_include_editor_ids=author_suggestion_include_editor_ids,
        recommended_person_ids=recommended_person_ids
    )


def get_recommendation_json(
        recommended_person_ids: list,
        author_suggestion_exclude_editor_ids: list,
        author_suggestion_include_editor_ids: list
) -> dict:
    return {
       'person_ids': recommended_person_ids,
       'recommendation_html': get_recommendation_html(
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_editor_ids,
            author_suggestion_include_editor_ids=author_suggestion_include_editor_ids,
            recommended_person_ids=recommended_person_ids
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
            recommended_person_ids=recommeded_senior_editor_ids
        )

        json_response_for_reviewing_editors = get_recommendation_json(
            author_suggestion_exclude_editor_ids=author_suggestion_exclude_reviewing_editors,
            author_suggestion_include_editor_ids=author_suggestion_include_reviewing_editors,
            recommended_person_ids=recommeded_reviewing_editor_ids
        )

        return jsonify(get_response_json(
            senior_editor_recommendation_json=json_response_for_senior_editors,
            reviewing_editor_recommendation_json=json_response_for_reviewing_editors
        ))

    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    main()
