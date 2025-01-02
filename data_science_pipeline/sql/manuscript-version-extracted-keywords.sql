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
)

SELECT
  * EXCEPT(version_id_row_number)
FROM t_manuscript_version_abstract_keywords
WHERE version_id_row_number = 1
ORDER BY version_id
