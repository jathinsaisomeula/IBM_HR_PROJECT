SELECT 
   COUNT (EmployeeNumber) AS employee_count
 FROM `sacred-pipe-454410-p7.ibm_hr.ibm project-1` 
 WHERE 
  Department IN ("Research & Development" , "Human Resources" , "Sales" ) 
 GROUP BY Department ;