SELECT
  p.product_id,
  p.name AS product_name,
  p.category AS product_category,
  SUM(fgc.total_quantity) AS total_quantity_sold,
  SUM(fgc.total_sales) AS total_sales_revenue
FROM {{ ref('fct_guest_checks') }} fgc
JOIN {{ ref('dim_product') }} p ON fgc.product_id = p.product_id
GROUP BY 
  p.product_id, 
  p.name, 
  p.category
ORDER BY 
  total_quantity_sold DESC
LIMIT 10;
