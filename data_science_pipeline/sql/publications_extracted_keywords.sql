WITH t_paper_extracted_keywords AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY pmid ORDER BY data_hub_imported_timestamp DESC) AS pmid_row_number
  FROM `{project}.{dataset}.data_science_disambiguated_editor_papers_abstract_keywords`
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
  REGEXP_EXTRACT(LOWER(paper_summary.doi), r'10.7554/elife.([0-9]{5,6})') AS manuscript_id,
  paper_summary.firstPublicationDate AS publication_date,
  paper_extracted_keywords.extracted_keywords AS abstract_keywords
FROM t_paper_extracted_keywords AS paper_extracted_keywords
JOIN t_paper_summary AS paper_summary
  ON paper_summary.pmid = paper_extracted_keywords.pmid
  AND paper_summary.pmid_row_number = 1
WHERE paper_extracted_keywords.pmid_row_number = 1
  AND ARRAY_LENGTH(paper_extracted_keywords.extracted_keywords) > 0
