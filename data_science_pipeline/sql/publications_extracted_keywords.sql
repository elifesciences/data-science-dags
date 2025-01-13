WITH t_paper_extracted_keywords AS (
  SELECT
    CAST(editor_paper_abstract_keywords_result.meta.pmid AS STRING) AS pmid,
    ARRAY(
      SELECT keyword
      FROM UNNEST(editor_paper_abstract_keywords_result.attributes.keywords)
    ) AS extracted_keywords,
    ROW_NUMBER() OVER(PARTITION BY editor_paper_abstract_keywords_result.meta.pmid ORDER BY imported_timestamp DESC) AS pmid_row_number
  FROM `{project}.{dataset}.keywords_from_disambiguated_editor_papers_abstract_batch` AS editor_paper_abstract_keywords_batch
  JOIN UNNEST(editor_paper_abstract_keywords_batch.data) AS editor_paper_abstract_keywords_result
),

t_paper_summary AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY pmid ORDER BY provenance.imported_timestamp DESC) AS pmid_row_number
  FROM `{project}.{dataset}.data_science_external_manuscript_summary`
)

SELECT
  CONCAT('pmid:', paper_extracted_keywords.pmid) AS publication_id,
  paper_extracted_keywords.pmid,
  paper_summary.doi,
  REGEXP_EXTRACT(LOWER(paper_summary.doi), r'10.7554/elife.([0-9]{{5,6}})') AS manuscript_id,
  paper_summary.firstPublicationDate AS publication_date,
  paper_extracted_keywords.extracted_keywords AS abstract_keywords
FROM t_paper_extracted_keywords AS paper_extracted_keywords
JOIN t_paper_summary AS paper_summary
  ON paper_summary.pmid = paper_extracted_keywords.pmid
  AND paper_summary.pmid_row_number = 1
WHERE paper_extracted_keywords.pmid_row_number = 1
  AND ARRAY_LENGTH(paper_extracted_keywords.extracted_keywords) > 0
