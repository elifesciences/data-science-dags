SELECT
    person.Name AS person_name,
    person.institution,
    person.Primary_Address.Country AS country,
    profile.Website_URL AS website,
    profile.PubMed_URL AS pubmed,
    profile.Current_Availability AS availability,
    event.*
FROM `{project}.{dataset}.mv_Editorial_Person` AS person
INNER JOIN `{project}.{dataset}.mv_Editorial_Editor_Profile` AS profile
ON person.person_id = profile.Person_ID
LEFT JOIN
(SELECT DISTINCT
    Person.Person_ID AS person_id,
    CAST(ROUND(PERCENTILE_CONT(
        Initial_Submission.Reviewing_Editor.Consultation.Days_To_Respond, 0.5
        ) OVER (PARTITION BY Person.Person_ID),1) AS STRING) AS days_to_respond,
    CAST(COUNT(DISTINCT Initial_Submission.Reviewing_Editor.Consultation.Request_Version_ID
        ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS requests,
    CAST(COUNT(DISTINCT Initial_Submission.Reviewing_Editor.Consultation.Response_Version_ID
        ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS responses,
    CAST(ROUND(AVG(Initial_Submission.Reviewing_Editor.Consultation.Has_Response_Ratio
        ) OVER (PARTITION BY Person.Person_ID)*100,1) AS STRING) AS response_rate,
    CAST(MAX(Full_Submission.Reviewing_Editor.Current_Assignment_Count
        ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS no_of_assigments,
    CAST(COUNT(DISTINCT Full_Submission.Reviewing_Editor.Assigned_Version_ID
        ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS no_of_full_submissions,
    CAST(PERCENTILE_CONT(
        Full_Submission.Reviewing_Editor.Submission_Received_To_Decision_Complete, 0.5
        ) OVER (PARTITION BY Person.Person_ID) AS STRING) AS decision_time,
    FROM `{project}.{dataset}.mv_Editorial_Editor_Workload_Event`,
    UNNEST(Person.Roles) AS person_role
    WHERE DATE(Event_Timestamp)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND person_role.Role_Name='Editorial Board Member'
    AND Person.Person_ID IN UNNEST(@person_ids)
) AS event
ON person.person_id = event.person_id
WHERE person.person_id IN UNNEST(@person_ids)
