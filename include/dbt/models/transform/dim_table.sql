WITH base_table AS (
    SELECT DISTINCT
        table_number,
        table_name,
        MAX(guest_check_id) AS latest_guest_check_id
    FROM {{ ref('raw_table') }}
    GROUP BY table_number, table_name
)
SELECT
    table_number AS table_id,
    table_name,
    latest_guest_check_id
FROM base_table;
