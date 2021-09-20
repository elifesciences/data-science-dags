WITH t_data_science_editor_pubmed_ids_with_rn AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY provenance.imported_timestamp DESC) as rn
    FROM `{project}.{dataset}.mv_data_science_editor_pubmed_ids` AS pubmed_ids
),

t_latest_data_science_editor_pubmed_ids AS (
    SELECT * FROM t_data_science_editor_pubmed_ids_with_rn
    WHERE rn = 1
)

SELECT * EXCEPT(rn)
FROM t_latest_data_science_editor_pubmed_ids
