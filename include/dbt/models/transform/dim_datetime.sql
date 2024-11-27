WITH base_datetime AS (
    SELECT DISTINCT
        datetime_id,
        date,
        year,
        month,
        day,
        hour,
        minute,
        is_weekend
    FROM {{ ref('raw_datetime') }}
)
SELECT
    datetime_id,
    date,
    year,
    month,
    day,
    hour,
    minute,
    is_weekend
FROM base_datetime;
