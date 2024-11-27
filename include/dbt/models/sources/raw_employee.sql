WITH raw_employee AS (
    SELECT DISTINCT
        empNum AS employee_id,
        chkEmpId AS check_employee_id,
        'Employee Name Placeholder' AS name,
        'Position Placeholder' AS position
    FROM {{ source('retail', 'raw_guest_checks') }}
    WHERE empNum IS NOT NULL
)
SELECT * FROM raw_employee;
