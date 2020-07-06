-- Main features:
--    - Disambiguates editor linked papers as far as possible
--      Gives each editor paper match a priority
--    - Select papers with increasing priority until at least a target number of papers is reached

WITH t_editor AS (
  SELECT
    editor.person_id,
    editor.relevant_pubmed_ids
  FROM `{project}.{dataset}.data_science_editor_pubmed_links` AS editor
),

t_pubmed_id_with_priority_by_person_id AS (
  SELECT DISTINCT editor.person_id, pmid, 1 AS priority
  FROM t_editor AS editor
  JOIN UNNEST(relevant_pubmed_ids) AS pmid

  UNION DISTINCT

  SELECT DISTINCT
    person_id,
    pmid,
    CASE
      WHEN has_matching_orcid THEN 1
      WHEN has_matching_last_name AND has_matching_first_name AND (has_matching_affiliation OR has_matching_previous_affiliation) THEN 2
      WHEN has_matching_last_name AND has_matching_first_name AND has_matching_postal_code THEN 3
      WHEN has_matching_last_name AND has_matching_first_name AND has_matching_city THEN 4
      WHEN has_matching_last_name AND has_matching_first_name AND has_matching_country THEN 5
      WHEN has_matching_last_name AND has_matching_first_name_letter AND (has_matching_affiliation OR has_matching_previous_affiliation) THEN 6
      ELSE 7
    END AS priority
  FROM `elife-data-pipeline.de_dev.data_science_disambiguated_editor_papers_details`
  WHERE NOT COALESCE(has_mismatching_orcid, FALSE)
),

t_priority_count_by_person_id AS (
  SELECT person_id, priority, COUNT(*) AS priority_count
  FROM t_pubmed_id_with_priority_by_person_id 
  GROUP BY person_id, priority
),

t_priority_count_and_total_priority_count_by_person_id AS (
  SELECT
    current_counts.*,
    (
      priority_count
      + (
        SELECT COALESCE(SUM(priority_count), 0)
        FROM t_priority_count_by_person_id AS higher_priority_counts
        WHERE higher_priority_counts.person_id = current_counts.person_id
        AND higher_priority_counts.priority < current_counts.priority
      )
    ) AS total_priority_count
  FROM t_priority_count_by_person_id AS current_counts
),

t_priority_below_and_above_target_count_by_person_id AS (
  SELECT
    person_id,
    MAX(IF(
      total_priority_count < {target_paper_count},
      priority,
      NULL
    )) AS priority_below_target_count,
    MIN(IF(
      (
        total_priority_count >= {target_paper_count}
        AND (
          -- upper limit, unless we are very sure (priority 1 or 2)
          total_priority_count < {max_paper_count}
          OR priority <= 2
        )
      ),
      priority,
      NULL
    )) AS priority_above_target_count
  FROM t_priority_count_and_total_priority_count_by_person_id
  GROUP BY person_id
),

t_max_preferred_priority_by_person_id AS (
  SELECT
    person_id,
    COALESCE(priority_above_target_count, priority_below_target_count) AS max_preferred_priority
  FROM t_priority_below_and_above_target_count_by_person_id
),

t_preferred_pubmed_id_by_person_id AS (
  SELECT DISTINCT
    max_preferred.person_id,
    paper.pmid
  FROM t_max_preferred_priority_by_person_id AS max_preferred
  JOIN t_pubmed_id_with_priority_by_person_id AS paper
    ON paper.person_id = max_preferred.person_id
    AND paper.priority <= max_preferred.max_preferred_priority
    AND paper.pmid IS NOT NULL
)

SELECT
  editor.person_id,
  ARRAY(
    SELECT pmid
    FROM t_preferred_pubmed_id_by_person_id AS preferred_paper
    WHERE preferred_paper.person_id = editor.person_id
  ) AS disambiguated_pubmed_ids
FROM t_editor AS editor
