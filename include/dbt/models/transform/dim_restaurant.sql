WITH base_restaurant AS (
    SELECT DISTINCT
        restaurant_id,
        region,
        city,
        name
    FROM {{ ref('raw_restaurant') }}
)
SELECT
    restaurant_id,
    region,
    city,
    name
FROM base_restaurant;
