WITH t_editor_pubmed_ids AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY provenance.imported_timestamp DESC) AS person_id_row_number
  FROM `{project}.{dataset}.mv_data_science_editor_pubmed_ids`
),

t_paper_summary AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY pmid ORDER BY provenance.imported_timestamp DESC) AS pmid_row_number
  FROM `{project}.{dataset}.data_science_external_manuscript_summary`
)

SELECT
  disambiguated_editor_papers.person_id,
  CONCAT('pmid:', pmid) AS publication_id,
  pmid,
  doi,
  REGEXP_EXTRACT(LOWER(paper_summary.doi), r'10.7554/elife.([0-9]{{5,6}})') AS manuscript_id,
  paper_summary.firstPublicationDate AS publication_date,
  (pmid IN UNNEST(editor_pubmed_links.relevant_pubmed_ids)) AS is_relevant_pubmed_id,
  (pmid IN UNNEST(editor_pubmed_ids.pubmed_ids)) AS is_search_pubmed_id

FROM `{project}.{dataset}.data_science_disambiguated_editor_papers` AS disambiguated_editor_papers
JOIN `{project}.{dataset}.data_science_editor_pubmed_links` AS editor_pubmed_links
  ON editor_pubmed_links.person_id = disambiguated_editor_papers.person_id
JOIN t_editor_pubmed_ids AS editor_pubmed_ids
  ON editor_pubmed_ids.person_id = disambiguated_editor_papers.person_id
  AND editor_pubmed_ids.person_id_row_number = 1 
JOIN UNNEST(disambiguated_editor_papers.disambiguated_pubmed_ids) AS pmid
JOIN t_paper_summary AS paper_summary 
  ON paper_summary.pmid = pmid
  AND paper_summary.pmid_row_number = 1
