WITH raw_restaurant AS (
    SELECT DISTINCT
        locRef AS region_id,
        rvcNum AS restaurant_id,
        'Region Placeholder' AS region,
        'City Placeholder' AS city,
        'Restaurant Name Placeholder' AS name
    FROM {{ source('raw_data', 'guest_checks') }}
    WHERE locRef IS NOT NULL
)
SELECT * FROM raw_restaurant;
