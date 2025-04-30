-- Selects the schema and name of all user tables (BASE TABLE) in the current database.
-- Ordered by schema, then table name for consistent output.
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE = 'BASE TABLE'
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME;