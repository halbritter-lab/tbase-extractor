-- Selects all columns (*) for a specific patient based on their PatientID.
-- Uses a parameter placeholder (?) for the PatientID to prevent SQL injection.
-- ** IMPORTANT: Verify 'dbo.Patient' schema and table name and 'PatientID' column name
-- ** match your actual database structure. **
SELECT 
    p.PatientID, p.Vorname, p.Name, p.Geburtsdatum, p.Grunderkrankung, p.ET_Grunderkrankung, p.Dauernotiz, p.Dauernotiz_Diagnose,
    d.ICD10, d.Bezeichnung AS DiagnoseBezeichnung
FROM
    dbo.Patient p
LEFT JOIN
    dbo.Diagnose d ON p.PatientID = d.PatientID
WHERE
    p.PatientID = ?