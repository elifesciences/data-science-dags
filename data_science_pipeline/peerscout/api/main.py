from flask import Flask, jsonify, request
import functools
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional
from werkzeug.exceptions import BadRequest, Unauthorized
import jsonschema
import json

LOGGER = logging.getLogger(__name__)

REQUEST_JSON_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'input-json-schema.json')


class EnvironmentVariableNames:
    PEERSCOUT_API_ACCESS_TOKEN = 'PEERSCOUT_API_ACCESS_TOKEN'


class HttpHeaderVariables:
    ACCESS_TOKEN = 'X-Access-Token'


def get_peerscout_api_access_token():
    return os.getenv(EnvironmentVariableNames.PEERSCOUT_API_ACCESS_TOKEN)


class RouteWrapper(ABC):
    @abstractmethod
    def wrap(self, fn):
        pass

    def __call__(self, fn):
        return self.wrap(fn)


class DummyRouteWrapper(RouteWrapper):
    def wrap(self, fn):
        return fn


class AccessControlRouteWrapper(RouteWrapper):
    def __init__(self, access_token: str):
        self.access_token = access_token

    def wrap(self, fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            LOGGER.debug('checking access (fn:%s)', fn)
            request_access_token = request.headers.get(HttpHeaderVariables.ACCESS_TOKEN)
            if request_access_token != self.access_token:
                raise Unauthorized()
            return fn(*args, **kwargs)
        return wrapper


def get_route_wrapper(access_token: Optional[str]) -> RouteWrapper:
    if not access_token:
        return DummyRouteWrapper()
    return AccessControlRouteWrapper(access_token)


def get_recommendation_html(person_ids: list) -> str:
    if not person_ids:
        return '<b>No</b> recommendation available'
    return '<b>The recommended editors are:</b> %s' % person_ids


def get_recommendation_json(person_ids: list) -> dict:
    return {
       'person_ids': person_ids,
       'recommendation_html': get_recommendation_html(person_ids)
    }


def get_response_json(
        senior_editor_person_ids: list,
        reviewing_editor_person_ids: list) -> dict:
    return {
        'senior_editor_recommendation': get_recommendation_json(senior_editor_person_ids),
        'reviewing_editor_recommendation': get_recommendation_json(reviewing_editor_person_ids)
    }


def create_app():
    route_wrapper = get_route_wrapper(
        get_peerscout_api_access_token()
    )

    app = Flask(__name__)

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
    @route_wrapper
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
        suggested_reviewing_editor_person_ids = author_suggestion['include_reviewing_editors_id']
        suggested_senior_editor_person_ids = author_suggestion['include_senior_editors_id']

        return jsonify(get_response_json(
            senior_editor_person_ids=suggested_senior_editor_person_ids if abstract else [],
            reviewing_editor_person_ids=suggested_reviewing_editor_person_ids if abstract else []
        ))

    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    main()
