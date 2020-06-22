WITH t_editor_with_orcids AS (
  SELECT
    Profile.Person_ID AS person_id,
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
  FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Profile
  JOIN `{project}.{dataset}.mv_person_v2` AS person
    ON person.person_id = Profile.person_id
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
)

SELECT
  person_id,
  ARRAY(
    SELECT DISTINCT pmid
    FROM t_pubmed_id_by_orcid AS paper
    WHERE paper.orcid = editor.orcid
  ) AS disambiguated_pubmed_ids
FROM t_editor_with_single_orcid AS editor
