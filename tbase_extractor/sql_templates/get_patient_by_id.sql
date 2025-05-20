-- Selects all columns (*) for a specific patient based on their PatientID.
-- Uses a parameter placeholder (?) for the PatientID to prevent SQL injection.
-- ** IMPORTANT: Verify 'dbo.Patient' schema and table name and 'PatientID' column name
-- ** match your actual database structure. **
SELECT 
    p.*  -- Get all columns to see complete patient data
FROM
    dbo.Patient p
WHERE
    p.PatientID = ?