SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'streaming_users'
ORDER BY ordinal_position;
