-- Fetches ALL patients. Use with caution on large tables!
-- ** IMPORTANT: Verify 'dbo.Patient' schema/table name matches your DB **
SELECT
    p.*
FROM
    dbo.Patient p;
