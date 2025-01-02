-- Main features:
--    - Returns Initial Submissions for the purpose of Senior Editor recommendation
--    - No older than a year
--    - Not have Senior Editor assigned for more than 30 days

WITH t_manuscript_version_abstract_keywords AS (
  SELECT
    manuscript_abstract_keywords.version_id,
    manuscript_abstract_keywords.extracted_keywords,
    ROW_NUMBER() OVER (
      PARTITION BY version_id
      ORDER BY data_hub_imported_timestamp DESC
    ) AS version_id_row_number
  FROM `{project}.{dataset}.manuscript_abstract_keywords` AS manuscript_abstract_keywords
  WHERE ARRAY_LENGTH(extracted_keywords) > 0
),

t_last_manuscript_version_abstract_keywords AS (
  SELECT
    * EXCEPT(version_id_row_number)
  FROM t_manuscript_version_abstract_keywords
  WHERE version_id_row_number = 1
  ORDER BY version_id
)

SELECT version.version_id, manuscript_version_abstract_keywords.extracted_keywords
, ARRAY( SELECT person_id FROM UNNEST(version.authors)) AS author_person_ids
FROM `{project}.{dataset}.mv_manuscript_version` AS version
JOIN t_last_manuscript_version_abstract_keywords AS manuscript_version_abstract_keywords
  ON manuscript_version_abstract_keywords.version_id = version.version_id
WHERE (version.overall_stage = 'Initial Submission' OR version.source_site_id = 'rp_site')
  AND (
    ARRAY_LENGTH(version.senior_editors) = 0
    OR TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP,
      (SELECT MAX(last_assigned_timestamp) FROM UNNEST(version.senior_editors)),
      DAY
    ) < 30
  )
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP, version.created_timestamp, DAY) < 365
  AND NOT is_withdrawn
  AND NOT is_deleted
