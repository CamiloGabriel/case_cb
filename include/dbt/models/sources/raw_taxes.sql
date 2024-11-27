WITH raw_taxes AS (
    SELECT DISTINCT
        guestCheckId AS guest_check_id,
        taxNum AS tax_id,
        txblSlsTtl AS taxable_sales_total,
        taxCollTtl AS tax_collected,
        taxRate AS tax_rate,
        type AS tax_type
    FROM {{ source('retail', 'raw_guest_checks') }}
    WHERE taxNum IS NOT NULL
)
SELECT * FROM raw_taxes;
