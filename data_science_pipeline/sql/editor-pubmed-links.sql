SELECT
  Person_ID AS person_id,
  Name AS name,
  Pubmed_URL AS pubmed_url,
  Relevant_Pubmed_URLs AS relevant_pubmed_urls
FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Editor
