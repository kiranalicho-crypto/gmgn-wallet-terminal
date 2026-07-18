SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM information_schema.columns
WHERE
    LOWER(table_schema) LIKE '%pump%'
    OR LOWER(table_name) LIKE '%pump%'
    OR LOWER(column_name) LIKE '%pump%'
ORDER BY
    table_schema,
    table_name,
    ordinal_position
