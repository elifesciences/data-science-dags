import logging
import requests

LOGGER = logging.getLogger(__name__)

request_json = """{
    "manuscript_id":"12345",
    "tracking_number":"12345_ABC",
    "revision_number":0,
    "abstract":"Taking advantage of mouse molecular and genetic tools, \
        the lab is addressing important questions about sensory system perception \
        from the level of the gene to the level of organismal behavior. \
        Using an integrative approach spanning molecular optogenetics and chemogenetics, \
        viral circuit tracing, in vivo and in vitro calcium imaging, and electrophysiology, \
        our overall goal is to increase our basic understanding of the mechanisms governing \
        encoding of somatosensory stimuli - with a particular focus on pain.",
    "title":"<b>Lorem Ipsum</b>",
    "author_suggestion":{
        "include_reviewing_editors_id":[
            "43038",
            "178962",
            "7970"
        ],
        "exclude_reviewing_editors_id":[
            "15332",
            "42473"
        ],
        "include_senior_editors_id":[
            "15332",
            "42473",
            "42011"
        ],
        "exclude_senior_editors_id":[
            "73636"
        ]
    }
}"""


def test_get_response_json():
    # to run test locally use url below:
    # url = 'http://localhost:8090/api/peerscout'

    url = 'http://peerscout-api:8080/api/peerscout'
    headers = {'Content-Type': 'application/json'}

    resp = requests.post(url, data=request_json, headers=headers)
    LOGGER.info('request: %s', resp.request)
    assert resp.status_code == 200
    LOGGER.info('reponse: %s', resp)
    resp_body = resp.json()
    assert '<h4>' in resp_body['reviewing_editor_recommendation']['recommendation_html']
