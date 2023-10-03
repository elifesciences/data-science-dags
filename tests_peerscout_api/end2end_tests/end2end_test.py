import os
import logging
import requests

LOGGER = logging.getLogger(__name__)

PEERSCOUT_API_URL_ENV_NAME = "PEERSCOUT_API_URL"
DEFAULT_PEERSCOUT_API_URL_ENV_VALUE = "http://localhost:8090/api/peerscout"


def get_api_url_env() -> str:
    return os.getenv(
        PEERSCOUT_API_URL_ENV_NAME,
        DEFAULT_PEERSCOUT_API_URL_ENV_VALUE
    )


def test_get_response_json():
    with open(
        './peerscout_api/example-data/peerscout-api-request1.json',
        encoding='utf-8'
    ) as request_json:
        headers = {'Content-Type': 'application/json'}
        url = get_api_url_env()
        LOGGER.info('api url : %s', url)
        resp = requests.post(url, data=request_json, headers=headers, timeout=60)
        LOGGER.info('request: %s', resp.request)
        assert resp.status_code == 200
        LOGGER.info('reponse: %s', resp)
        resp_body = resp.json()
        assert '<h4 style=' in resp_body['reviewing_editor_recommendation']['recommendation_html']
