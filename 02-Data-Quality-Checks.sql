--- Purpose:
-- Generate core data quality metrics for RESP-NET COVID-19 data.
-- This view summarizes missingness, zero rates, and coverage gaps by snapshot date and key analytic dimensions.

CREATE OR REPLACE VIEW
`project2-482922.Covid_Rates.vw_covid_data_quality` AS

WITH base AS (
  SELECT
    snapshot_date,
    State,
    AgeCategory_Legend,
    Type,
    month_date,
    MonthlyRate
  FROM `project2-482922.Covid_Rates.covid_snapshots`
),

flags AS (
  SELECT
    *,
    CASE WHEN MonthlyRate IS NULL THEN 1 ELSE 0 END AS is_missing_rate,
    CASE WHEN MonthlyRate = 0 THEN 1 ELSE 0 END AS is_zero_rate
  FROM base
)

SELECT
  snapshot_date,
  State,
  AgeCategory_Legend,
  Type,

  COUNT(*) AS total_records,
  SUM(is_missing_rate) AS missing_rate_count,
  SUM(is_zero_rate) AS zero_rate_count,

  SAFE_DIVIDE(SUM(is_missing_rate), COUNT(*)) AS pct_missing_rate,
  SAFE_DIVIDE(SUM(is_zero_rate), COUNT(*)) AS pct_zero_rate,

  COUNT(DISTINCT month_date) AS months_present,
  MIN(month_date) AS first_month,
  MAX(month_date) AS last_month
FROM flags
GROUP BY
  snapshot_date,
  State,
  AgeCategory_Legend,
  Type;
