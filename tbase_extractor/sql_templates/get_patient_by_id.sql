-- Selects all columns (*) for a specific patient based on their PatientID.
-- Uses a parameter placeholder (?) for the PatientID to prevent SQL injection.
-- ** IMPORTANT: Verify 'dbo.Patient' schema and table name and 'PatientID' column name
-- ** match your actual database structure. **
SELECT 
    p.PatientID, p.Vorname, p.Name, p.Geburtsdatum, p.Grunderkrankung, p.ET_Grunderkrankung, p.Dauernotiz, p.Dauernotiz_Diagnose
FROM
    dbo.Patient p
WHERE
    p.PatientID = ?
