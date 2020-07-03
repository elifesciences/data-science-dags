from data_science_pipeline.utils.pubmed import (
    normalize_url,
    is_ncbi_domain_url,
    get_ncbi_pubmed_article_id,
    get_ncbi_pubmed_article_ids,
    is_ncbi_bibliography_url,
    is_ncbi_search_url,
    get_ncbi_search_term,
    parse_term_query,
    combined_page_url_href,
    PubmedBibliographyPage
)


PMID_1 = '12345671'
PMID_2 = '12345672'


class TestNormalizeUrl:
    def test_should_not_change_https(self):
        assert normalize_url(
            'https://host/path'
        ) == 'https://host/path'

    def test_should_replace_http_with_https(self):
        assert normalize_url(
            'http://host/path'
        ) == 'https://host/path'

    def test_should_add_https(self):
        assert normalize_url(
            'host/path'
        ) == 'https://host/path'


class TestIsNcbiDomainUrl:
    def test_should_return_true_for_www_ncbi_nlm_nih_gov(self):
        assert is_ncbi_domain_url('https://www.ncbi.nlm.nih.gov/?query')

    def test_should_return_true_for_pubmed_ncbi_nlm_nih_gov(self):
        assert is_ncbi_domain_url('https://pubmed.ncbi.nlm.nih.gov/?query')

    def test_should_return_false_for_tinyurl(self):
        assert not is_ncbi_domain_url('https://tinyurl.com/path')


class TestGetNcbiPubmedArticleId:
    def test_should_return_none_if_not_pubmed_article_url(self):
        assert (
            get_ncbi_pubmed_article_id('https://www.ncbi.nlm.nih.gov/other/123')
            is None
        )

    def test_should_extract_pubmed_id_from_pubmed_path_url(self):
        assert (
            get_ncbi_pubmed_article_id('https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1)
            == PMID_1
        )

    def test_should_extract_pubmed_id_from_pubmed_subdomain_url(self):
        assert (
            get_ncbi_pubmed_article_id('https://pubmed.ncbi.nlm.nih.gov/' + PMID_1)
            == PMID_1
        )

    def test_should_extract_pubmed_id_from_http_url(self):
        assert (
            get_ncbi_pubmed_article_id('http://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1)
            == PMID_1
        )

    def test_should_extract_pubmed_id_from_http_url_without_www_prefix(self):
        assert (
            get_ncbi_pubmed_article_id('http://ncbi.nlm.nih.gov/pubmed/' + PMID_1)
            == PMID_1
        )

    def test_should_strip_slash(self):
        assert (
            get_ncbi_pubmed_article_id('https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1 + '/')
            == PMID_1
        )


class TestGetNcbiPubmedArticleIds:
    def test_should_extract_multiple_pubmed_article_ids(self):
        assert (
            get_ncbi_pubmed_article_ids([
                'https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1,
                'https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_2
            ]) == [PMID_1, PMID_2]
        )

    def test_should_skip_non_pubmed_article_urls(self):
        assert (
            get_ncbi_pubmed_article_ids([
                'https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1,
                'https://www.ncbi.nlm.nih.gov/other/123',
                'https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_2
            ]) == [PMID_1, PMID_2]
        )

    def test_should_remove_line_feed(self):
        assert (
            get_ncbi_pubmed_article_ids([
                'https://www.ncbi.nlm.nih.gov/pubmed/' + PMID_1 + '\n',
            ]) == [PMID_1]
        )


class TestIsNcbiBibliographyUrl:
    def test_should_return_true_for_myncbi_bibliography_url(self):
        assert is_ncbi_bibliography_url(
            'https://www.ncbi.nlm.nih.gov/myncbi/user-id1/bibliography/public/?query'
        )

    def test_should_return_true_for_myncbi_bibliography_url_containing_id(self):
        assert is_ncbi_bibliography_url(
            'https://www.ncbi.nlm.nih.gov/myncbi/user-id1/bibliography/12345/public/?query'
        )

    def test_should_return_true_for_myncbi_collection_url(self):
        # collection url redirects to bibliography
        assert is_ncbi_bibliography_url(
            'https://www.ncbi.nlm.nih.gov/myncbi/browse/collection/12345/?query'
        )

    def test_should_return_false_for_pubmed_search_url(self):
        assert not is_ncbi_bibliography_url(
            'https://www.ncbi.nlm.nih.gov/pubmed/?term=query'
        )

    def test_should_return_false_for_non_ncbi_url(self):
        assert not is_ncbi_bibliography_url(
            'https://other/bibliography/12345'
        )


class TestIsNcbiSearchUrl:
    def test_should_return_true_for_pubmed_search_url(self):
        assert is_ncbi_search_url(
            'https://www.ncbi.nlm.nih.gov/pubmed/?term=query'
        )

    def test_should_return_false_if_term_is_blank(self):
        assert not is_ncbi_search_url(
            'https://www.ncbi.nlm.nih.gov/pubmed/?term='
        )

    def test_should_return_false_for_myncbi_bibliography_url(self):
        assert not is_ncbi_search_url(
            'https://www.ncbi.nlm.nih.gov/myncbi/user-id1/bibliography/public/?term=query'
        )

    def test_should_return_false_for_non_ncbi_url(self):
        assert not is_ncbi_search_url(
            'https://other/bibliography/12345?term=query'
        )


class TestGetNcbiSearchTerm:
    def test_should_extract_term_from_pubmed_search_url(self):
        assert get_ncbi_search_term(
            'https://www.ncbi.nlm.nih.gov/pubmed/?term=query1'
        ) == 'query1'


class TestParseTermQuery:
    def test_should_parse_author_name_with_author_suffix(self):
        result = parse_term_query('Smith J[Author]')
        assert result.get('include', {}).get('author') == ['Smith J']

    def test_should_parse_author_name_with_auth_suffix(self):
        result = parse_term_query('Smith J[auth]')
        assert result.get('include', {}).get('author') == ['Smith J']

    def test_should_parse_author_name_with_au_suffix(self):
        result = parse_term_query('Smith J[au]')
        assert result.get('include', {}).get('author') == ['Smith J']

    def test_should_parse_author_with_included_and_excluded_ids(self):
        result = parse_term_query(' '.join([
            'Smith J[Author]',
            'OR include_id1[pmid]',
            'OR include_id2[pmid]',
            'NOT exclude_id1[pmid]',
            'NOT exclude_id2[pmid]'
        ]))
        assert result.get('include', {}).get('author') == ['Smith J']
        assert result.get('include', {}).get('pmid') == ['include_id1', 'include_id2']
        assert result.get('exclude', {}).get('pmid') == ['exclude_id1', 'exclude_id2']


class TestCombinedPageUrlHref:
    def test_should_preserve_existing_query_params(self):
        assert combined_page_url_href(
            'http://host/path?sortby=pubDate',
            '?page=2'
        ) == 'http://host/path?sortby=pubDate&page=2'

    def test_should_update_existing_page(self):
        assert combined_page_url_href(
            'http://host/path?sortby=pubDate&page=1',
            '?page=2'
        ) == 'http://host/path?sortby=pubDate&page=2'


class TestPubmedBibliographyPage:
    def test_should_find_pubmed_ids(self):
        html_content = '\n'.join([
            '<html><body><div class="citations other">',
            '<div><span class="pmid">PubMed PMID: %s<span></div>' % PMID_1,
            '<div><span class="pmid">PubMed PMID:\n%s<span></div>' % PMID_2,
            '</div></body></html>'
        ])
        page = PubmedBibliographyPage(html_content)
        assert page.pmids == [PMID_1, PMID_2]

    def test_should_find_next_enabled_page_url(self):
        html_content = '\n'.join([
            '<html><body><div class="citations other">',
            '<div class="pager"><form id="pager">',
            '<a class="nextPage enabled" href="?page=2">Next page</a>',
            '</form></div>',
            '</div></body></html>'
        ])
        page = PubmedBibliographyPage(html_content)
        assert page.next_page_href == '?page=2'
        assert page.get_next_page_href(
            'http://host/path?sortby=pubDate'
        ) == 'http://host/path?sortby=pubDate&page=2'

    def test_should_return_none_if_next_page_is_disabled(self):
        html_content = '\n'.join([
            '<html><body><div class="citations other">',
            '<div class="pager"><form id="pager">',
            '<a class="nextPage disabled" href="?page=2">Next page</a>',
            '</form></div>',
            '</div></body></html>'
        ])
        page = PubmedBibliographyPage(html_content)
        assert page.next_page_href is None
        assert page.get_next_page_href(
            'http://host/path?sortby=pubDate'
        ) is None
