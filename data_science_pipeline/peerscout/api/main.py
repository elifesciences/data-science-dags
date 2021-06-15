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

output = {
    "reviewing_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": "The recommended editors are ..."
    },
    "senior_editor_recommendation": {
        "person_ids": [],
        "recommendation_html": "The recommended editors are ..."
    }
}


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


def create_app():
    route_wrapper = get_route_wrapper(
        get_peerscout_api_access_token()
    )

    app = Flask(__name__)

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
        with open('./data_science_pipeline/peerscout/api/input-json-schema.json') as f:
            json_schema = json.load(f)

        try:
            jsonschema.validate(data, json_schema)
        except jsonschema.exceptions.ValidationError as e:
            print("invalid JSON:", e)

        try:
            abstract = data['abstract']
            reviewing_editors = data['include_reviewing_editors_id']
            senior_editors = data['include_senior_editors_id']
        except KeyError as e:
            raise BadRequest() from e

        if abstract != "":
            output['reviewing_editor_recommendation']['person_ids'] = reviewing_editors
            output['senior_editor_recommendation']['person_ids'] = senior_editors
            return jsonify(output)
        else:
            raise BadRequest('no valid abstract provided')
    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    main()
