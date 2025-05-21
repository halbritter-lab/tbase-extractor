-- Fetches patients whose DOB year falls within a given range.
-- Parameters: ? (start_year), ? (end_year)
-- ** IMPORTANT: Verify 'dbo.Patient' schema/table name and 'Geburtsdatum' column name match your DB **
SELECT
    p.PatientID, p.Vorname, p.Name, p.Geburtsdatum, p.Grunderkrankung, p.ET_Grunderkrankung, p.Dauernotiz, p.Dauernotiz_Diagnose,
    d.ICD10, d.Bezeichnung AS DiagnoseBezeichnung
FROM
    dbo.Patient p
LEFT JOIN
    dbo.Diagnose d ON p.PatientID = d.PatientID
WHERE
    YEAR(p.Geburtsdatum) BETWEEN ? AND ?;
