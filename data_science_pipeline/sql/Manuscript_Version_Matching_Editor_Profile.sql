WITH t_initial_submission_version_id_including_null AS (
  -- add NULL to also have one entry for the general editor search ("no manuscript selected")
  SELECT NULL AS version_id
  
  UNION ALL

  SELECT version_id
  FROM `{project}.{dataset}.v_manuscript_version_last_editor_assigned_timestamp` AS manuscript_version
  WHERE manuscript_version.overall_stage = 'Initial Submission'
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), manuscript_version.stages[SAFE_OFFSET(0)].stage_timestamp, DAY) <= 90
  AND (
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), manuscript_version.stages[SAFE_OFFSET(0)].stage_timestamp, DAY) <= 30
    OR NOT EXISTS (SELECT 1 FROM UNNEST(manuscript_version.stages) WHERE stage_name = 'Senior Editor Assigned')
  )
  AND NOT is_withdrawn
  AND NOT is_deleted
),

t_recommendations AS (
  SELECT
    Editor.*,
    manuscript_version.manuscript_id AS Manuscript_ID,
    manuscript_version.version_id AS Version_ID,
    manuscript_version.long_manuscript_identifier AS Long_Manuscript_Identifier,
    COALESCE(
      CONCAT(manuscript_version.long_manuscript_identifier, ' - ', manuscript_version.manuscript_title),
      '~ No Manuscript Selected ~'
    ) AS Display_Manuscript_Identifier,
    manuscript_version.overall_stage AS Overall_Stage,
    manuscript_version.manuscript_title AS Manuscript_Title,
    manuscript_version.abstract AS Abstract,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT keyword
        FROM UNNEST(matching_keywords)
      ),
      ', '
    ) AS Matching_Keywords_CSV,
    editor_recommendation.score AS Matching_Score,
    (
      MAX(editor_recommendation.score) OVER(
        PARTITION BY manuscript_version.version_id
      ) IS NOT NULL
    ) AS Has_Recommendation,
    manuscript_version.stages[SAFE_OFFSET(0)].stage_name AS Last_Stage_Name,
    (
      SELECT stage_timestamp
      FROM UNNEST(stages)
      WHERE stage_name = 'Senior Editor Assigned'
      ORDER BY stage_timestamp DESC
      LIMIT 1
    ) AS Senior_Editor_Assigned_Timestamp
  FROM `{project}.{dataset}.mv_Editorial_Editor_Profile` AS Editor
  CROSS JOIN t_initial_submission_version_id_including_null AS version_ids
  LEFT JOIN `{project}.{dataset}.v_manuscript_version_last_editor_assigned_timestamp` AS manuscript_version
    ON manuscript_version.version_id = version_ids.version_id
  LEFT JOIN `{project}.{dataset}.data_science_editor_recommendation` AS editor_recommendation
    ON editor_recommendation.person_id = Editor.Person_ID
    AND editor_recommendation.version_id = version_ids.version_id
  WHERE Editor.Role_Name = 'Senior Editor'
)

SELECT
  *,
  IF(
    Version_ID IS NULL,
    Display_Manuscript_Identifier,
    CONCAT(
      Long_Manuscript_Identifier,
      IF(Senior_Editor_Assigned_Timestamp IS NOT NULL, ' (SE assigned)', ''),
      IF(
        NOT Has_Recommendation,
        ' (no recommendation yet)',
        ''
      ),
      ' - ',
      Manuscript_Title
    )
  ) AS Display_Manuscript_Identifier_With_State
FROM t_recommendations
ORDER BY Version_ID DESC, Matching_Score DESC
