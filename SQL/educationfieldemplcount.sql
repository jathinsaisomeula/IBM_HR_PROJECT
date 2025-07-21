SELECT  
 EducationField ,
 COUNT (EmployeeNumber) AS Employee_count
FROM `sacred-pipe-454410-p7.ibm_hr.ibm project-1` 
GROUP BY 
 EducationField