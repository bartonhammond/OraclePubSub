CREATE OR REPLACE PROCEDURE HR.EMPLOYEE_SELECT_BY_PK
     ( 
        p_EMPLOYEE_ID           IN  EMPLOYEES.EMPLOYEE_ID%type  ,                  
        p_LASTNAME             OUT  EMPLOYEES.LAST_NAME%type     , 
        p_FIRSTNAME            OUT  EMPLOYEES.FIRST_NAME%type     , 
        p_EMAIL                OUT  EMPLOYEES.EMAIL%type                
     ) 
AS 
BEGIN  
    SELECT  
           LAST_NAME                      , 
           FIRST_NAME                     , 
           EMAIL           
    INTO   
           p_LASTNAME                      , 
           p_FIRSTNAME                     , 
           p_EMAIL           
    FROM   EMPLOYEES
    WHERE  EMPLOYEE_ID  = p_EMPLOYEE_ID  ; 

EXCEPTION 
        WHEN OTHERS THEN 
             RAISE_APPLICATION_ERROR (-20001, 
                                      p_EMPLOYEE_ID || ':$:' || SQLERRM, TRUE) ; 

END EMPLOYEE_SELECT_BY_PK ; 
/