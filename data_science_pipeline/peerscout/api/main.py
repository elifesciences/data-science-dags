from flask import Flask, jsonify, request
import functools
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional
from werkzeug.exceptions import BadRequest, Unauthorized


LOGGER = logging.getLogger(__name__)

output1 = {
    "reviewing_editor_recommendation": {
        "person_ids": [1, 2, 3],
        "recommendation_html": "The recommended editors are ..."
    },
    "senior_editor_recommendation": {
        "person_ids": [1, 2, 3],
        "recommendation_html": "The recommended editors are ..."
    }
}

output2 = {
    "reviewing_editor_recommendation2": {
        "person_ids": [1, 2, 3],
        "recommendation_html": "The recommended editors are ..."
    },
    "senior_editor_recommendation2": {
        "person_ids": [1, 2, 3],
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

    @app.route('/api/peerscout', methods=['POST'])
    @route_wrapper
    def _home():
        data = request.get_json(force=True)
        try:
            abstract = data['abstract']
        except KeyError as e:
            raise BadRequest() from e
        if abstract == 'output1':
            return jsonify(output1)
        elif abstract == 'output2':
            return jsonify(output2)
        else:
            raise BadRequest('no valid abstract provided')
    return app


def main():
    app = create_app()
    app.run(port='8080', host='0.0.0.0', threaded=False)


if __name__ == "__main__":
    main()
