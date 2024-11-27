WITH base_guest_checks AS (
    SELECT
        gc.guestCheckId AS guest_check_id,
        gc.opnUTC AS datetime_id,
        gc.locRef AS restaurant_id,
        gc.gstCnt AS guest_count,
        gc.chkTtl AS check_total,
        COALESCE(SUM(dl.dspQty), 0) AS total_quantity,
        COALESCE(SUM(dl.dspTtl), 0) AS total_sales,
        COALESCE(SUM(t.tax_collected), 0) AS total_taxes,
        COALESCE(gc.dscTtl, 0) AS discount_total
    FROM {{ source('raw_data', 'guest_checks') }} gc
    LEFT JOIN {{ ref('raw_detail_lines') }} dl ON gc.guestCheckId = dl.guest_check_id
    LEFT JOIN {{ ref('raw_taxes') }} t ON gc.guestCheckId = t.guest_check_id
    GROUP BY
        gc.guestCheckId,
        gc.opnUTC,
        gc.locRef,
        gc.gstCnt,
        gc.chkTtl,
        gc.dscTtl
)
SELECT
    gc.guest_check_id,
    dt.datetime_id,
    r.restaurant_id,
    e.employee_id,
    t.tax_id,
    tb.table_number AS table_id,
    gc.guest_count,
    gc.check_total,
    gc.total_quantity,
    gc.total_sales,
    gc.total_taxes,
    gc.discount_total
FROM base_guest_checks gc
LEFT JOIN {{ ref('dim_datetime') }} dt ON gc.datetime_id = dt.datetime_id
LEFT JOIN {{ ref('dim_restaurant') }} r ON gc.restaurant_id = r.restaurant_id
LEFT JOIN {{ ref('dim_employee') }} e ON gc.restaurant_id = e.restaurant_id
LEFT JOIN {{ ref('dim_tax') }} t ON gc.guest_check_id = t.guest_check_id
LEFT JOIN {{ ref('dim_table') }} tb ON gc.restaurant_id = tb.restaurant_id;
