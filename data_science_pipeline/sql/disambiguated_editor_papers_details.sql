-- Main features:
--    - Provides matching details for each of the editor linked pubmed ids
--      for the purpose of disambiguation
--    - For each paper it also needs to find the best matching author
--      which will then provide the paper match details

WITH t_editor_pubmed_ids AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY name ORDER BY provenance.imported_timestamp DESC) AS name_row_number
  FROM `{project}.{dataset}.mv_data_science_editor_pubmed_ids`
),

t_external_manuscript_summary AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY pmid ORDER BY provenance.imported_timestamp DESC) AS pmid_row_number
  FROM `{project}.{dataset}.data_science_external_manuscript_summary`
),

t_editor_extra AS (
  SELECT
    Profile.Role_Name AS role_name,
    editor_pubmed_ids.person_id,
    editor_pubmed_ids.name,
    editor_pubmed_ids.parsed_search_term.include.author[SAFE_OFFSET(0)] AS search_author_name,
    person.first_name,
    person.last_name,
    person.institution,
    (
      SELECT AS STRUCT *
      FROM UNNEST(person.addresses) AS address
      ORDER BY address.start_timestamp DESC
      LIMIT 1
    ) AS primary_address,
    (
      SELECT reference_value
      FROM UNNEST(person.external_references)
      WHERE is_enabled
        AND reference_type = 'ORCID'
      ORDER BY start_timestamp, modified_timestamp
      LIMIT 1
    ) AS verified_orcid,
    (
      SELECT reference_value
      FROM UNNEST(person.external_references)
      WHERE is_enabled
        AND reference_type = 'UNVERIFIED_ORCID'
      ORDER BY start_timestamp, modified_timestamp
      LIMIT 1
    ) AS unverified_orcid,
    ARRAY(
      SELECT DISTINCT pubmed_id
      FROM UNNEST(pubmed_ids) AS pubmed_id
      WHERE pubmed_id IS NOT NULL
    ) AS pubmed_ids,
  FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Profile
  JOIN t_editor_pubmed_ids AS editor_pubmed_ids
    ON editor_pubmed_ids.person_id = Profile.Person_ID
  JOIN `{project}.{dataset}.mv_person_v2` AS person
    ON person.person_id = editor_pubmed_ids.person_id
),

t_editor_extra_single_orcid AS (
  SELECT
    *,
    COALESCE(verified_orcid, unverified_orcid) AS orcid
  FROM t_editor_extra
),

t_papers_by_orcid AS (
  SELECT
    COALESCE(
      IF(author.authorId.type = 'ORCID', NULLIF(author.authorId.value, ''), NULL),
      'NONE'
    ) AS author_orcid,
    ARRAY_AGG(
      DISTINCT
      IF(
        IF(author.authorId.type = 'ORCID', NULLIF(author.authorId.value, ''), NULL) IS NOT NULL,
        ARRAY_TO_STRING(author.authorAffiliationsList.authorAffiliation, ', '),
        NULL
      )
      IGNORE NULLS
    ) AS affiliation_strings,
    ARRAY_TO_STRING(ARRAY_AGG(
      DISTINCT
      IF(
        IF(author.authorId.type = 'ORCID', NULLIF(author.authorId.value, ''), NULL) IS NOT NULL,
        ARRAY_TO_STRING(author.authorAffiliationsList.authorAffiliation, ', '),
        NULL
      )
      IGNORE NULLS
    ), '; ') AS all_affiliation_strings_csv
  FROM t_external_manuscript_summary AS manuscript_summary
  JOIN UNNEST(authorList.author) AS author
  GROUP BY author_orcid
),

t_editor_papers_with_author_affiliation_string AS (
  SELECT
    editor.* EXCEPT(pubmed_ids),
    editor_pubmed_id AS pmid,
    doi,
    title,
    DATE(DATETIME(manuscript_summary.firstPublicationDate)) AS first_publication_date,
    authorString AS author_string,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT TRIM(CONCAT(author.firstName, ' ', author.lastName))
        FROM UNNEST(authorList.author) AS author
      ),
      '; '
    ) AS first_last_name_author_string,
    ARRAY(
      SELECT AS STRUCT
        author.firstName AS author_first_name,
        author.lastName AS author_last_name,
        author.fullName AS author_full_name,
        author.initials AS author_initials,
        ARRAY_TO_STRING(author.authorAffiliationsList.authorAffiliation, ', ') AS affiliation_string,
        IF(author.authorId.type = 'ORCID', NULLIF(author.authorId.value, ''), NULL) AS author_orcid
      FROM UNNEST(authorList.author) AS author
    ) AS author_candidates
  FROM t_editor_extra_single_orcid AS editor
  JOIN UNNEST(editor.pubmed_ids) AS editor_pubmed_id
  JOIN t_external_manuscript_summary AS manuscript_summary
    ON manuscript_summary.pmid = editor_pubmed_id
),

t_editor_papers_with_author_candiate AS (
  SELECT
    editor_papers.* EXCEPT(author_candidates),
    ARRAY(
      SELECT AS STRUCT
        author.*,
        (author.author_orcid = orcid) AS has_matching_orcid,
        (author.author_orcid != orcid) AS has_mismatching_orcid,
        (author_last_name = last_name) AS has_matching_last_name,
        (author_first_name = first_name) AS has_matching_first_name,
        (SUBSTR(author_first_name, 1, 1) = SUBSTR(first_name, 1, 1)) AS has_matching_first_name_letter,
        (STRPOS(affiliation_string, institution) > 0) AS has_matching_affiliation,
        (STRPOS(affiliation_string, primary_address.country) > 0) AS has_matching_country,
        (STRPOS(affiliation_string, primary_address.city) > 0) AS has_matching_city,
        (STRPOS(affiliation_string, primary_address.postal_code) > 0) AS has_matching_postal_code,
        (STRPOS(all_affiliation_strings_csv, institution) > 0) AS has_matching_previous_affiliation,
      FROM UNNEST(author_candidates) AS author
    JOIN t_papers_by_orcid AS previous_papers_by_orcid
      ON previous_papers_by_orcid.author_orcid = COALESCE(author.author_orcid, 'NONE')
    ) AS author_candidates
  FROM t_editor_papers_with_author_affiliation_string AS editor_papers
),

t_editor_papers_with_author_candiate_with_score AS (
  SELECT
    editor_papers.* EXCEPT(author_candidates),
    ARRAY(
      SELECT AS STRUCT
        author.*,
        -- calculate a score that we use to rank authors of a paper to the editor
        -- this could potentially use the same priority that we later use to select papers
        (
          IF(COALESCE(has_matching_orcid, FALSE), 50, 0)
          + IF(COALESCE(has_mismatching_orcid, FALSE), -5, 0)
          + IF(COALESCE(has_matching_last_name, FALSE), 20, 0)
          + IF(COALESCE(has_matching_first_name, FALSE), 10, 0)
          + IF(COALESCE(has_matching_first_name_letter, FALSE), 5, 0)
          + IF(COALESCE(has_matching_affiliation, FALSE), 4, 0)
          + IF(COALESCE(has_matching_previous_affiliation, FALSE), 2, 0)
          + IF(COALESCE(has_matching_postal_code, FALSE), 3, 0)
          + IF(COALESCE(has_matching_city, FALSE), 2, 0)
          + IF(COALESCE(has_matching_country, FALSE), 1, 0)
        ) AS author_match_score
      FROM UNNEST(author_candidates) AS author
      ORDER BY author_match_score DESC
    ) AS author_candidates
  FROM t_editor_papers_with_author_candiate AS editor_papers
),


t_editor_papers_with_best_author_candiate AS (
  SELECT
    editor_papers.*,
    IF(
      author_candidates[SAFE_OFFSET(0)].author_match_score > 0 OR ARRAY_LENGTH(author_candidates) = 1,
      author_candidates[SAFE_OFFSET(0)],
      NULL
    ) AS best_author_candidate
  FROM t_editor_papers_with_author_candiate_with_score AS editor_papers
)

SELECT
  editor_papers.* EXCEPT(best_author_candidate),
  best_author_candidate.*
FROM t_editor_papers_with_best_author_candiate AS editor_papers
