-- Selects all columns for patients matching First Name, Last Name, and Date of Birth.
-- Uses parameter placeholders (?) to prevent SQL injection.
-- ** IMPORTANT: Verify 'dbo.Patient' schema/table name and column names
-- ** ('Vorname', 'Name', 'Geburtsdatum') match your actual database structure. **
SELECT
    T1.PatientID, T1.Vorname, T1.Name, T1.Geburtsdatum, T1.Grunderkrankung, T1.ET_Grunderkrankung, T1.Dauernotiz, T1.Dauernotiz_Diagnose
FROM
    dbo.Patient AS T1
WHERE
    T1.Vorname = ?       -- Parameter 1: First Name
    AND T1.Name = ?      -- Parameter 2: Last Name
    AND T1.Geburtsdatum = ? -- Parameter 3: Date of Birth
;