import logging
import re
from urllib.parse import urlparse, parse_qs, urlencode, urljoin
from typing import Iterable, List

import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)


NCBI_DOMAIN_NAME = 'ncbi.nlm.nih.gov'
NCBI_PUBMED_URL_PREFIX_LIST = [
    'https://www.ncbi.nlm.nih.gov/pubmed/',
    'https://ncbi.nlm.nih.gov/pubmed/',
    'https://pubmed.ncbi.nlm.nih.gov/'
]

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


def is_ncbi_pubmed_article_url(url: str) -> str:
    return any(
        url.startswith(prefix)
        for prefix in NCBI_PUBMED_URL_PREFIX_LIST
    )


def get_ncbi_pubmed_article_id(url: str) -> str:
    url = normalize_url(url)
    if not is_ncbi_pubmed_article_url(url):
        return None
    return urlparse(url).path.rstrip('/').split('/')[-1].strip()


def get_ncbi_pubmed_article_ids(urls: List[str]) -> List[str]:
    return list(filter(bool, [
        get_ncbi_pubmed_article_id(url)
        for url in urls
    ]))


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


def combined_page_url_href(base_url: str, page_href) -> str:
    # Note: the actual next page link seem to be loosing the sorting
    #   we will keep all query parameters from the base url except the page
    page_list = parse_qs(urlparse(page_href).query).get('page')
    page = page_list[0] if page_list else '1'
    return urljoin(
        base_url,
        '?' + urlencode({
            **parse_qs(urlparse(base_url).query),
            'page': page
        }, doseq=True)
    )


class PubmedBibliographyNextPageLink:
    def __init__(self, next_page_soup: BeautifulSoup):
        self.next_page_soup = next_page_soup

    @property
    def href(self) -> str:
        if not self.next_page_soup:
            return None
        return self.next_page_soup['href']

    def get_href(self, base_url: str) -> str:
        if not self.next_page_soup:
            return None
        return combined_page_url_href(base_url, self.href)


class PubmedBibliographyPage:
    def __init__(self, html_content: str):
        self.html_content = html_content
        self.page_soup = BeautifulSoup(html_content, features='lxml')

    def parse_pmid_text(self, pmid_text: str) -> str:
        LOGGER.debug('pmid_text: %s', pmid_text)
        m = re.match(r'.*\b(\d+)\b.*', str(pmid_text), re.DOTALL)
        if m:
            LOGGER.debug('m.groups: %s', m.groups())
            return m.group(1)
        LOGGER.debug('no match')
        return None

    @property
    def pmids(self) -> List[str]:
        citations_soup = self.page_soup.find('div', class_='citations')
        pmid_soups = citations_soup.find_all(class_='pmid')
        return [self.parse_pmid_text(e.text) for e in pmid_soups]

    @property
    def next_page(self) -> PubmedBibliographyNextPageLink:
        return PubmedBibliographyNextPageLink(
            self.page_soup.select_one('#pager .nextPage.enabled')
        )

    @property
    def next_page_href(self) -> bool:
        return self.next_page.href

    def get_next_page_href(self, base_url: str) -> str:
        return self.next_page.get_href(base_url)


class PubmedBibliographyScraper:
    def __init__(self, session: requests.Session):
        self.session = session

    def get_html(self, url: str) -> str:
        response = self.session.get(url)
        response.raise_for_status()
        response_text = response.text
        LOGGER.info('retrieved html from: %s (%d chars)', url, len(response_text))
        return response_text

    def iter_pages(self, url: str) -> PubmedBibliographyPage:
        page = PubmedBibliographyPage(self.get_html(url))
        yield page
        previous_url = url
        next_page_url = page.get_next_page_href(url)
        while next_page_url and next_page_url != previous_url:
            page = PubmedBibliographyPage(self.get_html(next_page_url))
            yield page
            previous_url = next_page_url
            next_page_url = page.get_next_page_href(url)

    def iter_pmids(self, url: str) -> Iterable[str]:
        for page in self.iter_pages(url):
            yield from page.pmids

    def get_pmids(self, url: str) -> List[str]:
        return list(self.iter_pmids(url))
