import logging
import json
from unittest.mock import patch, MagicMock
from typing import Dict, Iterator

from flask import Flask
from flask.testing import FlaskClient

import pytest

from data_science_pipeline.peerscout.api.main import (
    create_app,
)


LOGGER = logging.getLogger(__name__)

INPUT_DATA_VALID = {
    "manuscript_id":"12345",
    "tracking_number":"12345_ABC",
    "revision_number":0,
    "abstract":"<p>Lorem ipsum dolor sit amet, <i>consectetur adipiscing elit</i></p>",
    "title":"<b>Lorem Ipsum</b>",
    "author_suggestion":{
      "include_reviewing_editors_id":[
        "43038",
        "178962",
        "7970"
      ],
      "exclude_reviewing_editors_id":[
        "1000"
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
}

INPUT_DATA_WTIHOUT_ABSTRACT = {**INPUT_DATA_VALID, "abstract":""}

NO_RECOMENDATION_RESPONSE = {
    "reviewing_editor_recommendation": {
        "person_ids": [], 
        "recommendation_html": "<b>No</b> recommendation available"
    }, 
    "senior_editor_recommendation": {
        "person_ids": [], 
        "recommendation_html": "<b>No</b> recommendation available"
    }
}

@pytest.fixture(name='test_client')
def _test_client() -> FlaskClient:
    app = create_app()
    return app.test_client()


def _get_json(response):
    return json.loads(response.data.decode('utf-8'))


def _get_ok_json(response):
    assert response.status_code == 200
    return _get_json(response)


class TestPeerscoutAPI:
    def test_should_have_access_for_status_page(self, test_client: FlaskClient):
        response = test_client.get('/api/status')
        assert _get_ok_json(response) == {"status": "OK"}

    def test_should_respond_badrequest_with_empty_json(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json={})
        assert response.status_code == 400

    def test_should_respond_with_proper_json(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json=INPUT_DATA_VALID)
        assert response.status_code == 200
    
    def test_should_respond_no_recomendation_with_empty_abstract(
        self,
        test_client: FlaskClient
    ):
        response = test_client.post('/api/peerscout', json=INPUT_DATA_WTIHOUT_ABSTRACT)
        assert response.json == NO_RECOMENDATION_RESPONSE
