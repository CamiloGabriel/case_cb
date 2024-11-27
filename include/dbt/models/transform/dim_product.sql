WITH base_product AS (
    SELECT DISTINCT
        product_id,
        name,
        category,
        AVG(price_including_tax) AS avg_price_including_tax,
        MAX(price_level) AS max_price_level,
        MAX(active_taxes) AS active_taxes,
        MAX(is_modified) AS is_modified
    FROM {{ ref('raw_product') }}
    GROUP BY product_id, name, category
)
SELECT
    product_id,
    name,
    category,
    avg_price_including_tax,
    max_price_level,
    active_taxes,
    is_modified
FROM base_product;
