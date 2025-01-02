-- Main features:
--    - Returns Initial Submissions for the purpose of Senior Editor recommendation
--    - No older than a year
--    - Not have Senior Editor assigned for more than 30 days

WITH t_manuscript_version_abstract_keywords AS (
  SELECT
    manuscript_abstract_keywords_result.meta.version_id,
    ARRAY(
      SELECT keyword
      FROM UNNEST(manuscript_abstract_keywords_result.attributes.keywords)
    ) AS extracted_keywords,
    ROW_NUMBER() OVER (
      PARTITION BY manuscript_abstract_keywords_result.meta.version_id
      ORDER BY imported_timestamp DESC
    ) AS version_id_row_number
  FROM `{project}.{dataset}.keywords_from_manuscript_abstract_batch` AS manuscript_abstract_keywords_batch
  JOIN UNNEST(manuscript_abstract_keywords_batch.data) AS manuscript_abstract_keywords_result
  WHERE ARRAY_LENGTH(manuscript_abstract_keywords_result.attributes.keywords) > 0
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
