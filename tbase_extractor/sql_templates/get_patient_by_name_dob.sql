-- Query that finds a patient by their first name, last name, and date of birth
SELECT 
    p.PatientID,
    p.Vorname AS FirstName,
    p.Name AS LastName,
    p.Geburtsdatum AS DateOfBirth,
    p.Geschlecht AS Gender,
    p.PLZ AS PostalCode,
    p.Ort AS City
FROM dbo.Patient p
WHERE 
    p.Vorname = ? 
    AND p.Name = ?
    AND p.Geburtsdatum = ?
