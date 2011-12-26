CREATE OR REPLACE
PROCEDURE hr.get_emp_rs (p_deptid   IN  employees.department_id%TYPE,
                      p_recordset OUT SYS_REFCURSOR) AS 
BEGIN 
  OPEN p_recordset FOR
    SELECT first_name,
           last_name,
           employee_id,
           email
    FROM   employees
    WHERE  department_id = p_deptid
    ORDER BY last_name, first_name ASC;
END get_emp_rs;
/