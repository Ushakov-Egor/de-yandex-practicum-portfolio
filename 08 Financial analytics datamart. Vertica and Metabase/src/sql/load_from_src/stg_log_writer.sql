INSERT INTO {table_name} (
    schema_name, 
    table_name,
    load_date, 
    load_end, 
    rows_loaded, 
    status, 
    error_message
)
VALUES (
    :schema_name,
    :table_name,
    CAST(:load_date AS timestamp),
    CAST(:load_end AS timestamp), 
    :rows_loaded, 
    :status, 
    :error_message
);