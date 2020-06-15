import pytest

from data_science_pipeline.utils.europepmc import (
    get_europepmc_author_query_string,
    get_europepmc_pmid_query_string,
    get_pmids_from_json_response
)


class TestGetEuropepmcAuthorQueryString:
    def test_should_fail_with_no_authors(self):
        with pytest.raises(ValueError):
            assert get_europepmc_author_query_string([])

    def test_should_convert_single_author(self):
        assert get_europepmc_author_query_string(
            ['Smith J']
        ) == '(AUTH:"Smith J") AND (SRC:"MED")'


class TestGetEuropepmcPmidQueryString:
    def test_should_fail_with_no_pmids(self):
        with pytest.raises(ValueError):
            assert get_europepmc_pmid_query_string([])

    def test_should_convert_single_pmid(self):
        assert get_europepmc_pmid_query_string(
            ['pmid1']
        ) == '(EXT_ID:"pmid1") AND (SRC:"MED")'

    def test_should_convert_multiple_pmids(self):
        assert get_europepmc_pmid_query_string(
            ['pmid1', 'pmid2', 'pmid3']
        ) == '(EXT_ID:"pmid1" OR EXT_ID:"pmid2" OR EXT_ID:"pmid3") AND (SRC:"MED")'


class TestGetPmidsFromJsonResponse:
    def test_should_extract_pmids(self):
        assert get_pmids_from_json_response({
            'resultList': {
                'result': [{
                    'pmid': 'pmid1'
                }, {
                    'pmid': 'pmid2'
                }]
            }
        }) == ['pmid1', 'pmid2']
