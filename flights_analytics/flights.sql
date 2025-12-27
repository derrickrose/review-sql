-- check parquet data from project databricks
-- ACTUAL ALIAS DEFINED ON A SELECT STATEMENT CANNOT BE USED IN A GROUP BY CLAUSE
-- TOTAL DURATION OF AIRCRAFT FLIGHTS BY YEAR
SELECT tailnum,
       Extract(year FROM year_month_dayofmonth) AS Year,
       Sum(crselapsedtime + arrdelay)           AS YearlyDuration
FROM   flights
WHERE  tailnum IS NOT NULL
       AND tailnum <> 'Unknow'
GROUP  BY Extract(year FROM year_month_dayofmonth),
          tailnum
ORDER  BY tailnum DESC,
          year DESC

-- TOTAL DURATION OF AIRCRAFT FLIGHTS BY MONTH
SELECT Concat(Extract(year FROM year_month_dayofmonth),
       Concat('_', Extract(month FROM
       year_month_dayofmonth)))                   AS YearMonth,
       tailnum,
       Count(crsdeptime)                          AS TotalFlights,
       Round(Sum(crselapsedtime + arrdelay) / 60) AS MonthlyDurationHours
FROM   flights
GROUP  BY Concat(Extract(year FROM year_month_dayofmonth),
          Concat('_', Extract(month FROM
          year_month_dayofmonth))),
          tailnum
ORDER  BY yearmonth DESC,
          tailnum DESC
