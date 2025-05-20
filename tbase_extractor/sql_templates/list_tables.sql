-- Query to list all user tables in the database
SELECT 
    t.TABLE_SCHEMA as SchemaName,
    t.TABLE_NAME as TableName,
    t.TABLE_TYPE as TableType,
    (
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS c 
        WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA 
        AND c.TABLE_NAME = t.TABLE_NAME
    ) as ColumnCount
FROM 
    INFORMATION_SCHEMA.TABLES t
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
ORDER BY 
    t.TABLE_SCHEMA,
    t.TABLE_NAME
