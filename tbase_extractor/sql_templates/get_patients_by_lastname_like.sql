-- Fetches patients whose Last Name matches a LIKE pattern.
-- Parameter: ? (lastname_pattern, e.g., 'Smith%')
-- ** IMPORTANT: Verify 'dbo.Patient' schema/table name and 'Name' column name match your DB **
SELECT
    p.PatientID, p.Vorname, p.Name, p.Geburtsdatum, p.Grunderkrankung, p.ET_Grunderkrankung, p.Dauernotiz, p.Dauernotiz_Diagnose
FROM
    dbo.Patient p
WHERE
    p.Name LIKE ?;
