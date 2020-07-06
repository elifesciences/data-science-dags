import re
import logging
from itertools import islice
from typing import Callable, Iterable, List

import requests

from data_science_pipeline.utils.requests import (
    requests_retry_session as _requests_retry_session
)


LOGGER = logging.getLogger(__name__)


EUROPEPMC_RETRY_STATUS_CODE_LIST = (429, 500, 502, 504)
# Note: we are using POST requests to avoid URL length limit, it is not stateful
EUROPEPMC_RETRY_METHOD_LIST = ('GET', 'HEAD', 'OPTIONS', 'POST')

EUROPEPMC_MAX_PAGE_SIZE = 1000

EUROPEPMC_START_CURSOR = '*'


def normalize_author_initials(author_name: str) -> str:
    return re.sub(r'(\s[A-Z])(\s)([A-Z])', r'\1\3', author_name, re.DOTALL)


def remove_comma(author_name: str) -> str:
    return author_name.replace(',', '')


def remove_double_quotes(author_name: str) -> str:
    return author_name.strip('"')


def normalize_author_name(author_name: str) -> str:
    return normalize_author_initials(
        remove_double_quotes(
            remove_comma(author_name.strip())
        )
    )


def get_europepmc_author_query_string(author_names: List[str]) -> str:
    if not author_names:
        raise ValueError('author names required')
    return '(%s) AND (SRC:"MED")' % ' OR '.join([
        'AUTH:"%s"' % normalize_author_name(author)
        for author in author_names
    ])


def get_europepmc_pmid_query_string(pmids: List[str]) -> str:
    if not pmids:
        raise ValueError('pmids required')
    return '(%s) AND (SRC:"MED")' % ' OR '.join([
        'EXT_ID:"%s"' % pmid for pmid in pmids
    ])


def get_pmids_from_json_response(json_response: dict) -> List[str]:
    if not json_response:
        return []
    return [
        item.get('pmid')
        for item in json_response.get('resultList', {}).get('result')
    ]


def get_manuscript_summary_from_json_response(json_response: dict) -> List[str]:
    if not json_response:
        return []
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


class EuropePMCApiResponsePage:
    def __init__(self, json_response: dict):
        self.json_response = json_response

    @property
    def total_count(self) -> int:
        return int(self.json_response.get('hitCount'))

    @property
    def current_cursor(self) -> str:
        return self.json_response.get('request', {}).get('cursorMark')

    @property
    def next_cursor(self) -> str:
        return self.json_response.get('nextCursorMark')

    @property
    def validated_next_cursor(self) -> str:
        next_cursor = self.next_cursor
        if next_cursor == self.current_cursor:
            return None
        return next_cursor

    @property
    def result_list(self) -> List[dict]:
        return self.json_response.get('resultList', {}).get('result', [])


class EuropePMCApiResponsePageIterator:
    def __init__(self, get_page_fn: Callable[[str], EuropePMCApiResponsePage]):
        self.get_page_fn = get_page_fn
        self.first_page = get_page_fn(EUROPEPMC_START_CURSOR)
        self.total_count = self.first_page.total_count if self.first_page else 0
        # Note: we are not always receiving exactly the same number of records
        self.min_received_count = self.total_count * 0.9

    def __iter__(self):
        if not self.first_page:
            return
        yield self.first_page
        current_cursor = self.first_page.validated_next_cursor
        current_count = len(self.first_page.result_list)
        while current_cursor:
            response_page = self.get_page_fn(current_cursor)
            if not response_page:
                return
            if response_page.current_cursor != current_cursor:
                raise AssertionError(
                    'expected current page cursor %s, but was: %s' % (
                        current_cursor, response_page.current_cursor
                    )
                )
            assert response_page.current_cursor == current_cursor
            yield response_page
            current_cursor = response_page.validated_next_cursor
            current_count += len(response_page.result_list)
        if current_count < self.min_received_count:
            raise AssertionError(
                ' '.join([
                    'expected %d items, but only received %d items',
                    '(last page current cursor: %s, last page next cursor: %s)'
                ]) % (
                    self.total_count, current_count,
                    response_page.current_cursor, response_page.next_cursor
                )
            )


class EuropePMCApi:
    def __init__(
            self,
            session: requests.Session,
            params: dict = None,
            on_error: callable = None):
        self.session = session
        self.params = params or {}
        self.on_error = on_error

    def query_page(
            self,
            query: str,
            result_type: str,
            output_format: str = 'json',
            cursor: str = EUROPEPMC_START_CURSOR,
            page_size: int = EUROPEPMC_MAX_PAGE_SIZE) -> EuropePMCApiResponsePage:
        LOGGER.debug('query: %s', query)
        data = {
            **self.params,
            'query': query,
            'format': output_format,
            'resultType': result_type,
            'pageSize': page_size,
            'cursorMark': cursor
        }
        try:
            response = requests.post(
                'https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST',
                data=data
            )
            response.raise_for_status()
            return EuropePMCApiResponsePage(response.json())
        except requests.HTTPError as e:
            if self.on_error is None:
                raise
            self.on_error(e, data=data)
            return None

    def iter_query_pages(
            self,
            *args,
            **kwargs) -> EuropePMCApiResponsePageIterator:
        return EuropePMCApiResponsePageIterator(
            lambda cursor: self.query_page(*args, cursor=cursor, **kwargs)
        )

    def iter_query_results(
            self,
            *args,
            limit: int = None,
            **kwargs) -> Iterable[dict]:
        return islice(
            (
                result
                for response_page in self.iter_query_pages(*args, **kwargs)
                for result in response_page.result_list
            ), limit
        )

    def iter_author_pmids(self, author_names: List[str], **kwargs) -> List[str]:
        return filter(
            bool,
            (item.get('pmid') for item in self.iter_query_results(
                get_europepmc_author_query_string(author_names),
                result_type='idlist',
                **kwargs
            ))
        )

    def get_author_pmids(self, *args, **kwargs) -> List[str]:
        return list(self.iter_author_pmids(*args, **kwargs))

    def get_summary_by_page_pmids(self, pmids: List[str]) -> List[dict]:
        if len(pmids) > EUROPEPMC_MAX_PAGE_SIZE:
            raise ValueError(
                'paging not supported, list of pmids must be less than %d'
                % EUROPEPMC_MAX_PAGE_SIZE
            )
        result = get_manuscript_summary_from_json_response(self.query_page(
            get_europepmc_pmid_query_string(pmids),
            result_type='core'
        ).json_response)
        if not result:
            LOGGER.warning('no results for %s', pmids)
        return result


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
