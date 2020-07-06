import pytest

from data_science_pipeline.utils.europepmc import (
    get_europepmc_author_query_string,
    get_europepmc_pmid_query_string,
    get_pmids_from_json_response,
    normalize_author_initials
)


class TestNormalizeAuthorInitials:
    def test_should_return_author_name_with_full_name(self):
        assert normalize_author_initials('Smith John') == 'Smith John'

    def test_should_return_author_name_with_simple_initial(self):
        assert normalize_author_initials('Smith J') == 'Smith J'

    def test_should_return_author_name_with_two_initials(self):
        assert normalize_author_initials('Smith JX') == 'Smith JX'

    def test_should_return_remove_space_between_initials(self):
        assert normalize_author_initials('Smith J X') == 'Smith JX'


class TestGetEuropepmcAuthorQueryString:
    def test_should_fail_with_no_authors(self):
        with pytest.raises(ValueError):
            assert get_europepmc_author_query_string([])

    def test_should_convert_single_author(self):
        assert get_europepmc_author_query_string(
            ['Smith J']
        ) == '(AUTH:"Smith J") AND (SRC:"MED")'

    def test_should_normalize_initials(self):
        assert get_europepmc_author_query_string(
            ['Smith J X']
        ) == '(AUTH:"Smith JX") AND (SRC:"MED")'

    def test_should_not_treat_capitalized_surname_as_initials(self):
        assert get_europepmc_author_query_string(
            ['SMITH J']
        ) == '(AUTH:"SMITH J") AND (SRC:"MED")'

    def test_should_remove_comma(self):
        assert get_europepmc_author_query_string(
            ['Smith, J']
        ) == '(AUTH:"Smith J") AND (SRC:"MED")'

    def test_should_strip_space(self):
        assert get_europepmc_author_query_string(
            [' Smith J ']
        ) == '(AUTH:"Smith J") AND (SRC:"MED")'

    def test_should_remove_double_quotes(self):
        assert get_europepmc_author_query_string(
            [' "Smith J" ']
        ) == '(AUTH:"Smith J") AND (SRC:"MED")'

    def test_should_transliterate_author_name_to_ascii(self):
        assert get_europepmc_author_query_string(
            [' "Śḿïth Ĵ" ']
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
