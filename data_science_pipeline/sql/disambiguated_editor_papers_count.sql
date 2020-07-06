WITH t_editor_pubmed_ids AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY name ORDER BY provenance.imported_timestamp DESC) AS name_row_number
  FROM `{project}.{dataset}.data_science_editor_pubmed_ids`
)

SELECT
  Profile.Person_ID AS person_id,
  Profile.Name AS name,
  ARRAY_LENGTH(papers.disambiguated_pubmed_ids) AS pubmed_count,
  ARRAY_LENGTH(editor_pubmed_links.relevant_pubmed_urls) AS relevant_pubmed_url_count,
  ARRAY_LENGTH(editor_pubmed_links.relevant_pubmed_ids) AS relevant_pubmed_id_count,
  ARRAY_LENGTH(editor_pubmed_ids.pubmed_ids) AS retrieved_pubmed_id_count,
  editor_pubmed_links.pubmed_url,
  editor_pubmed_links.search_term
FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Profile
LEFT JOIN `{project}.{dataset}.data_science_disambiguated_editor_papers` AS papers
  ON papers.person_id = Profile.Person_ID
LEFT JOIN `{project}.{dataset}.data_science_editor_pubmed_links` AS editor_pubmed_links
  ON editor_pubmed_links.person_id = Profile.Person_ID
LEFT JOIN t_editor_pubmed_ids AS editor_pubmed_ids
  ON editor_pubmed_ids.person_id = Profile.Person_ID
WHERE Profile.Name IS NOT NULL
ORDER BY Profile.Name
