from typing import List

import requests

from data_science_pipeline.utils.requests import (
    requests_retry_session as _requests_retry_session
)


EUROPEPMC_RETRY_STATUS_CODE_LIST = (429, 500, 502, 504)
# Note: we are using POST requests to avoid URL length limit, it is not stateful
EUROPEPMC_RETRY_METHOD_LIST = ('GET', 'HEAD', 'OPTIONS', 'POST')

EUROPEPMC_MAX_PAGE_SIZE = 1000


def get_europepmc_author_query_string(author_names: List[str]) -> str:
    if not author_names:
        raise ValueError('author names required')
    return '(%s) AND (SRC:"MED")' % ' OR '.join([
        'AUTH:"%s"' % author for author in author_names
    ])


def get_europepmc_pmid_query_string(pmids: List[str]) -> str:
    if not pmids:
        raise ValueError('pmids required')
    return '(%s) AND (SRC:"MED")' % ' OR '.join([
        'EXT_ID:"%s"' % pmid for pmid in pmids
    ])


def get_pmids_from_json_response(json_response: dict) -> List[str]:
    return [
        item.get('pmid')
        for item in json_response.get('resultList', {}).get('result')
    ]


def get_manuscript_summary_from_json_response(json_response: dict) -> List[str]:
    return [
        {
            'source': item.get('source'),
            'pmid': item.get('pmid'),
            'pmcid': item.get('pmcid'),
            'doi': item.get('doi'),
            'title': item.get('title'),
            'authorString': item.get('authorString'),
            'authorList': item.get('authorList'),
            'abstractText': item.get('abstractText'),
            'firstPublicationDate': item.get('firstPublicationDate')
        }
        for item in json_response.get('resultList', {}).get('result')
    ]


class EuropePMCApi:
    def __init__(self, session: requests.Session, params: dict = None):
        self.session = session
        self.params = params or {}

    def query(
            self,
            query: str,
            result_type: str,
            output_format: str = 'json',
            page_size: int = EUROPEPMC_MAX_PAGE_SIZE):
        response = requests.post(
            'https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST',
            data={
                **self.params,
                'query': query,
                'format': output_format,
                'resultType': result_type,
                'pageSize': page_size
            }
        )
        response.raise_for_status()
        return response.json()

    def get_author_pmids(self, author_names: List[str]) -> List[str]:
        return get_pmids_from_json_response(self.query(
            get_europepmc_author_query_string(author_names),
            result_type='idlist'
        ))

    def get_summary_by_page_pmids(self, pmids: List[str]) -> List[dict]:
        if len(pmids) > EUROPEPMC_MAX_PAGE_SIZE:
            raise ValueError(
                'paging not supported, list of pmids must be less than %d'
                % EUROPEPMC_MAX_PAGE_SIZE
            )
        return get_manuscript_summary_from_json_response(self.query(
            get_europepmc_pmid_query_string(pmids),
            result_type='core'
        ))

    def iter_get_summary_by_pmids(self, pmids: List[str]) -> List[dict]:
        return get_manuscript_summary_from_json_response(self.query(
            get_europepmc_pmid_query_string(pmids),
            result_type='core'
        ))


def europepmc_requests_retry_session(
        *args,
        status_forcelist=EUROPEPMC_RETRY_STATUS_CODE_LIST,
        method_whitelist=EUROPEPMC_RETRY_METHOD_LIST,
        **kwargs):
    return _requests_retry_session(
        *args,
        status_forcelist=status_forcelist,
        method_whitelist=method_whitelist,
        **kwargs
    )
