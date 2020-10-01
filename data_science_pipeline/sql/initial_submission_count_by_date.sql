SELECT
  DATE(QC_Complete_Timestamp) AS initial_submission_date,
  COUNT(DISTINCT Manuscript_ID) AS manuscript_count
FROM `{project}.{dataset}.mv_Editorial_Manuscript_Version`
WHERE Overall_Stage = 'Initial Submission'
  AND Position_In_Overall_Stage = 1
  AND DATE(QC_Complete_Timestamp) < DATE(CURRENT_TIMESTAMP())
GROUP BY initial_submission_date
ORDER BY initial_submission_date DESC
