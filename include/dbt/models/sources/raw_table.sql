WITH raw_table AS (
    SELECT DISTINCT
        guestCheckId AS guest_check_id,
        tblNum AS table_number,
        tblName AS table_name
    FROM {{ source('retail', 'raw_guest_checks') }}
    WHERE tblNum IS NOT NULL
)
SELECT * FROM raw_table;
