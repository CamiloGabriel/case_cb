WITH base_tax AS (
    SELECT
        tax_id,
        AVG(tax_rate) AS avg_tax_rate,
        SUM(taxable_sales_total) AS total_taxable_sales,
        SUM(tax_collected) AS total_tax_collected,
        MAX(tax_type) AS tax_type
    FROM {{ ref('raw_taxes') }}
    GROUP BY tax_id
)
SELECT
    tax_id,
    avg_tax_rate,
    total_taxable_sales,
    total_tax_collected,
    tax_type
FROM base_tax;
