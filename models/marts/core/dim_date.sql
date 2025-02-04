WITH CTE_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2010-01-01') AS CAL_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))  -- Number of days after reference date in previous line
  )
  SELECT CAL_DATE
        ,to_char(CAL_DATE::date,'yyyymmdd') as date_key
        ,YEAR(CAL_DATE) as year
        ,MONTH(CAL_DATE) as month
        ,MONTHNAME(CAL_DATE) as month_name
        ,DAY(CAL_DATE) as day
        ,DAYOFWEEK(CAL_DATE) as day_of_week
        ,WEEKOFYEAR(CAL_DATE) as week_of_year
        ,DAYOFYEAR(CAL_DATE) as day_of_year
    FROM CTE_DATE