WITH raw_product AS (
    SELECT DISTINCT
        miNum AS product_id,
        'Product Name Placeholder' AS name,
        'Category Placeholder' AS category,
        inclTax AS price_including_tax,
        prcLvl AS price_level,
        activeTaxes AS active_taxes,
        modFlag AS is_modified
    FROM {{ source('retail', 'raw_guest_checks') }}
    WHERE miNum IS NOT NULL
)
SELECT * FROM raw_product;
