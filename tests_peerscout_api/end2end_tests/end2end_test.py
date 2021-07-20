import logging
import requests

LOGGER = logging.getLogger(__name__)

# to run the test locally use the url below:
# URL = 'http://localhost:8090/api/peerscout'
URL = 'http://peerscout-api:8080/api/peerscout'


def test_get_response_json():
    with open('./peerscout_api/example-data/peerscout-api-request1.json') as request_json:
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(URL, data=request_json, headers=headers)
        LOGGER.info('request: %s', resp.request)
        assert resp.status_code == 200
        LOGGER.info('reponse: %s', resp)
        resp_body = resp.json()
        assert '<h4>' in resp_body['reviewing_editor_recommendation']['recommendation_html']
