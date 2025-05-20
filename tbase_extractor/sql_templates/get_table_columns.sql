-- see sql_templates/get_table_columns.sql in project root

-- Fetches column names and data types for a specific table in the database
-- Parameters: TABLE_NAME, TABLE_SCHEMA
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?;
