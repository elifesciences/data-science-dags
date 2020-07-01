SELECT
  Person_ID AS person_id,
  Name AS name
FROM `{project}.{dataset}.mv_Editorial_Editor_Profile`
WHERE Role_Name = 'Reviewing Editor'
