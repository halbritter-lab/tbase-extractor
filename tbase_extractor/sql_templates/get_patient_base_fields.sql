-- Selects base patient fields without any WHERE clause.
-- Intended to be appended with dynamic WHERE conditions in Python.
SELECT
    p.PatientID,
    p.Vorname,
    p.Name,
    p.Geburtsdatum,
    p.Grunderkrankung,
    p.ET_Grunderkrankung,
    p.Dauernotiz,
    p.Dauernotiz_Diagnose
FROM
    dbo.Patient p
