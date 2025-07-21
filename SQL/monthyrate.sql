SELECT 
  Count (MonthlyRate)
 FROM `sacred-pipe-454410-p7.ibm_hr.ibm project-1` 
 WHERE 
  Department = "Research & Development"
  AND MonthlyRate < 5000