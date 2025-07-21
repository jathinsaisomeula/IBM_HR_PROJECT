SELECT
  Department,
  Gender, -- Added comma
  EducationField,
  AVG(MonthlyRate) AS avg_monthly_rate,
  AVG(MonthlyIncome) AS avg_monthly_income,
  AVG(DailyRate) AS avg_daily_rate,
  AVG(HourlyRate) AS avg_hourly_rate
FROM `sacred-pipe-454410-p7.ibm_hr.ibm project-1`
WHERE
  Gender IN ('Male', 'Female') -- Corrected to allow both genders if needed, otherwise remove WHERE clause
GROUP BY
  Department,
  EducationField,
  Gender -- Added Gender to GROUP BY
ORDER BY
  avg_monthly_income DESC; -- Corrected to use alias for sorting, pick primary sort key