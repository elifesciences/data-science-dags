import re
from urllib.parse import urlparse, parse_qs

import requests


NCBI_DOMAIN_NAME = 'ncbi.nlm.nih.gov'


AUTHOR_TERM_FIELD = 'author'

TERM_FIELD_MAP = {
    'au': AUTHOR_TERM_FIELD,
    'auth': AUTHOR_TERM_FIELD
}


def normalize_url(url: str) -> str:
    if '://' not in url:
        return 'https://' + url
    return url.replace('http://', 'https://')


def resolve_url(url: str) -> str:
    # Follow redirect to resolve URL, without requesting the target URL
    response = requests.head(url, allow_redirects=False)
    # Don't raise for status, we will just keep the passed in url
    return response.headers.get('Location') or url


def is_ncbi_domain_url(url: str) -> bool:
    return urlparse(url.lower()).hostname.endswith(NCBI_DOMAIN_NAME)


def is_ncbi_bibliography_url(url: str) -> bool:
    if not is_ncbi_domain_url(url):
        return False
    path_fragments = urlparse(url).path.split('/')
    return (
        'bibliography' in path_fragments
        or 'collection' in path_fragments
    )


def get_ncbi_search_term(url: str) -> str:
    if not is_ncbi_domain_url(url) or is_ncbi_bibliography_url(url):
        return False
    terms = parse_qs(urlparse(url).query).get('term')
    return terms[0] if terms else ''


def is_ncbi_search_url(url: str) -> bool:
    return bool(get_ncbi_search_term(url))


def resolve_url_if_not_ncbi_domain(url: str) -> str:
    if is_ncbi_domain_url(url):
        return url
    return resolve_url(url)


def parse_term_item(term_item: str):
    term_item = term_item.strip()
    m = re.match(r'([^\[]+)(?:\[(.*)\])?', term_item)
    if not m:
        return term_item, AUTHOR_TERM_FIELD
    term_field = (m.group(2) or AUTHOR_TERM_FIELD).lower()
    return m.group(1), TERM_FIELD_MAP.get(term_field, term_field)


def parse_term_query(term_query: str) -> dict:
    result = {}
    operator = 'OR'
    for item in re.split(r'(OR|NOT)', term_query):
        item = item.strip()
        if item in {'OR', 'NOT'}:
            operator = item
            continue
        value, value_type = parse_term_item(item)
        if operator == 'NOT':
            result.setdefault('exclude', {}).setdefault(value_type, []).append(value)
        else:
            result.setdefault('include', {}).setdefault(value_type, []).append(value)
    return result
