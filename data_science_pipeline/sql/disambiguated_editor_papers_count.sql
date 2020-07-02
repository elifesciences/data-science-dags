SELECT
  Profile.Name AS name,
  ARRAY_LENGTH(papers.disambiguated_pubmed_ids) AS pubmed_count
FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Profile
LEFT JOIN `{project}.{dataset}.data_science_disambiguated_editor_papers` AS papers
  ON papers.person_id = Profile.Person_ID
WHERE Profile.Name IS NOT NULL
ORDER BY Profile.Name
