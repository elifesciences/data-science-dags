SELECT DISTINCT
  DATE(Processing_Summary.Publication.VOR_Timestamp) AS VOR_Date,
  CAST(ROUND(PERCENTILE_CONT(Processing_Summary.Accept_Decision_To_VOR, 0.5) OVER(
      PARTITION BY DATE(Publication.VOR_Timestamp)
  )) AS INT64) AS Accept_Decision_To_VOR
FROM `{project}.{dataset}.v_Editorial_Manuscript_Processing_Summary` AS Processing_Summary
WHERE DATE(Processing_Summary.Publication.VOR_Timestamp) <= DATE(CURRENT_TIMESTAMP())
  AND Processing_Summary.Full_Submission_Received_To_First_Full_Decision IS NOT NULL
ORDER BY VOR_Date DESC
