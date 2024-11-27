WITH base_employee AS (
    SELECT DISTINCT
        employee_id,
        check_employee_id,
        name,
        position
    FROM {{ ref('raw_employee') }}
)
SELECT
    employee_id,
    check_employee_id,
    name,
    position
FROM base_employee;
