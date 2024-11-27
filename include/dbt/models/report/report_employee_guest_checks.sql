SELECT
  e.position,
  e.employee_id,
  COUNT(gc.guest_check_id) AS total_guest_checks,
  SUM(gc.total_sales) AS total_revenue
FROM {{ ref('fct_guest_checks') }} gc
JOIN {{ ref('dim_employee') }} e ON gc.employee_id = e.employee_id
GROUP BY e.position, e.employee_id
ORDER BY total_revenue DESC
LIMIT 10;
