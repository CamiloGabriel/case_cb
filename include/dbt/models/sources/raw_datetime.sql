WITH raw_datetime AS (
    SELECT DISTINCT
        opnUTC AS datetime_id,
        CAST(opnUTC AS DATE) AS date,
        EXTRACT(YEAR FROM opnUTC) AS year,
        EXTRACT(MONTH FROM opnUTC) AS month,
        EXTRACT(DAY FROM opnUTC) AS day,
        EXTRACT(HOUR FROM opnUTC) AS hour,
        EXTRACT(MINUTE FROM opnUTC) AS minute,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM opnUTC) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM {{ source('retail', 'raw_guest_checks') }}
    WHERE opnUTC IS NOT NULL
)
SELECT * FROM raw_datetime;
