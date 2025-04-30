-- Selects all columns (*) for a specific patient based on their PatientID.
-- Uses a parameter placeholder (?) for the PatientID to prevent SQL injection.
-- ** IMPORTANT: Verify 'dbo.Patient' schema and table name and 'PatientID' column name
-- ** match your actual database structure. **
SELECT
    T1.*
FROM
    dbo.Patient AS T1
WHERE
    T1.PatientID = ?
;