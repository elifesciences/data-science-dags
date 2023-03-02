WITH t_related_person_id AS (
  SELECT DISTINCT Version_ID AS version_id, 'Reviewing Editor' AS relationship_type, Person.Person_ID
  FROM `{project}.{dataset}.mv_Editorial_All_Manuscript_Version` AS Version
  JOIN UNNEST(Version.Reviewing_Editors) AS Person
)

SELECT
  Version.Manuscript_ID AS manuscript_id,
  Version.Version_ID AS version_id,
  Version.Overall_Stage AS overall_stage,
  Version.QC_Complete_Timestamp AS qc_complete_timestamp,
  Version.Position_In_Overall_Stage AS position_in_overall_stage,
  related_person_id.relationship_type,
  related_person_id.person_id AS person_id,
  Related_Person.Name AS name,
  ARRAY_TO_STRING(ARRAY(SELECT Role_Name FROM UNNEST(Related_Person.Roles)), '|') AS person_roles,
  ARRAY_TO_STRING(ARRAY(SELECT Keyword FROM UNNEST(Related_Person.Keywords)), '|') AS person_keywords,
  ARRAY_TO_STRING(ARRAY(SELECT Subject_Area_Name FROM UNNEST(Related_Person.Subject_Areas)), '|') AS person_subject_areas

FROM `{project}.{dataset}.mv_Editorial_All_Manuscript_Version` AS Version
JOIN t_related_person_id AS related_person_id
  ON related_person_id.version_Id = Version.Version_ID
JOIN `{project}.{dataset}.mv_Editorial_Person` AS Related_Person
  ON Related_Person.Person_ID = related_person_id.person_id
ORDER BY Manuscript_ID, Version_ID, relationship_type, person_id
