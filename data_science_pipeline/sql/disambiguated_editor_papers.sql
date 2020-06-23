WITH t_editor_with_orcids AS (
  SELECT
    editor.person_id,
    editor.relevant_pubmed_ids,
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
    ) AS unverified_orcid
  FROM `{project}.{dataset}.data_science_editor_pubmed_links` AS editor
  JOIN `{project}.{dataset}.mv_person_v2` AS person
    ON person.person_id = editor.person_id
),

t_editor_with_single_orcid AS (
  SELECT
    *,
    COALESCE(verified_orcid, unverified_orcid) AS orcid
  FROM t_editor_with_orcids
),

t_pubmed_id_by_orcid AS (
  SELECT
    author.authorId.value AS orcid,
    manuscript_summary.pmid
  FROM `{project}.{dataset}.data_science_external_manuscript_summary` AS manuscript_summary
  JOIN UNNEST(manuscript_summary.authorList.author) AS author
  WHERE author.authorId.type = 'ORCID'
    AND pmid IS NOT NULL
),

t_pubmed_id_by_person_id AS (
  SELECT DISTINCT editor.person_id, pmid
  FROM t_editor_with_single_orcid AS editor
  JOIN t_pubmed_id_by_orcid AS paper
    ON paper.orcid = editor.orcid

  UNION DISTINCT

  SELECT DISTINCT editor.person_id, pmid
  FROM t_editor_with_single_orcid AS editor
  JOIN UNNEST(relevant_pubmed_ids) AS pmid
)

SELECT
  editor.person_id,
  ARRAY_AGG(pmid IGNORE NULLS) AS disambiguated_pubmed_ids
FROM t_editor_with_single_orcid AS editor
LEFT JOIN t_pubmed_id_by_person_id AS paper
  ON paper.person_id = editor.person_id
GROUP BY editor.person_id
