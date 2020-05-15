WITH t_manuscript_version_abstract_keywords AS (
  SELECT
    LPAD(CAST(manuscript_abstract_keywords.manuscript_id AS STRING), 5, '0') AS manuscript_id,
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
FROM `{project}.{dataset}.mv_manuscript_version` AS version
JOIN t_last_manuscript_version_abstract_keywords AS manuscript_version_abstract_keywords
  ON manuscript_version_abstract_keywords.version_id = version.version_id
WHERE version.overall_stage = 'Initial Submission'
  AND ARRAY_LENGTH(version.senior_editors) = 0
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP, version.created_timestamp, DAY) < 365
