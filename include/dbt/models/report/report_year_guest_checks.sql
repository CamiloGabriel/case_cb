SELECT
  dt.year,
  dt.month,
  COUNT(DISTINCT gc.guest_check_id) AS num_guest_checks,
  SUM(gc.check_total) AS total_revenue
FROM {{ ref('fct_guest_checks') }} gc
JOIN {{ ref('dim_datetime') }} dt ON gc.datetime_id = dt.datetime_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;
