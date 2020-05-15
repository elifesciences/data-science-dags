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
)

SELECT
  * EXCEPT(version_id_row_number)
FROM t_manuscript_version_abstract_keywords
WHERE version_id_row_number = 1
ORDER BY version_id
