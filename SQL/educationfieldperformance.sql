SELECT 
 EducationField , 
  AVG (PerformanceRating) AS avg_performance_rating ,
  AVG (PercentSalaryHike) AS avg_percent_salary_hike,
  AVG (RelationshipSatisfaction) AS avg_relationship_satisfaction ,
  AVG (WorklifeBalance) AS avg_worklife_balance 
 FROM `sacred-pipe-454410-p7.ibm_hr.ibm project-1` 
 GROUP BY
  EducationField