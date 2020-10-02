SELECT DISTINCT
  DATE(First_Full_Submission.Decision_Sent_Timestamp) AS Full_Decision_Sent_Date,
  CAST(ROUND(PERCENTILE_CONT(Processing_Summary.Full_Submission_Received_To_First_Full_Decision, 0.5) OVER(
      PARTITION BY DATE(First_Full_Submission.Decision_Sent_Timestamp)
  )) AS INT64) AS Full_Submission_Received_To_First_Full_Decision
FROM `{project}.{dataset}.v_Editorial_Manuscript_Processing_Summary` AS Processing_Summary
WHERE DATE(Processing_Summary.First_Full_Submission.Decision_Sent_Timestamp) <= DATE(CURRENT_TIMESTAMP())
  AND Processing_Summary.Full_Submission_Received_To_First_Full_Decision IS NOT NULL
ORDER BY Full_Decision_Sent_Date DESC
