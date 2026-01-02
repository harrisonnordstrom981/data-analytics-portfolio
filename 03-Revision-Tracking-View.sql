-- 03_revision_tracking.sql
-- Purpose:
-- Track how preliminary RESP-NET values change across snapshot dates.
-- Compares each snapshot to the immediately previous snapshot for the same
-- State / month / age group / type, and computes revision magnitude.
--
-- Why it matters:
-- Public health surveillance data are often preliminary and revised.
-- Revision tracking quantifies instability and highlights where values change.

CREATE OR REPLACE VIEW
`project2-482922.Covid_Rates.vw_covid_revisions` AS

WITH ordered AS (
  SELECT
    snapshot_date,
    State,
    month_date,
    AgeCategory_Legend,
    Sex_Label,
    Race_Label,
    Type,
    MonthlyRate,

    -- previous snapshot for the same series
    LAG(snapshot_date) OVER (
      PARTITION BY State, month_date, AgeCategory_Legend, Sex_Label, Race_Label, Type
      ORDER BY snapshot_date
    ) AS prev_snapshot_date,

    LAG(MonthlyRate) OVER (
      PARTITION BY State, month_date, AgeCategory_Legend, Sex_Label, Race_Label, Type
      ORDER BY snapshot_date
    ) AS prev_monthly_rate
  FROM `project2-482922.Covid_Rates.covid_snapshots`
  WHERE month_date IS NOT NULL
),

revisions AS (
  SELECT
    snapshot_date,
    prev_snapshot_date,

    State,
    month_date,
    AgeCategory_Legend,
    Sex_Label,
    Race_Label,
    Type,

    MonthlyRate AS current_rate,
    prev_monthly_rate AS previous_rate,

    -- revision magnitude
    (MonthlyRate - prev_monthly_rate) AS revision_abs,
    SAFE_DIVIDE((MonthlyRate - prev_monthly_rate), prev_monthly_rate) AS revision_pct,

    -- did it change?
    CASE
      WHEN prev_monthly_rate IS NULL THEN 0
      WHEN MonthlyRate = prev_monthly_rate THEN 0
      ELSE 1
    END AS flag_changed,

    -- material revision flag (tune thresholds as you like)
    CASE
      WHEN prev_monthly_rate IS NULL THEN 0
      WHEN ABS(MonthlyRate - prev_monthly_rate) >= 1 THEN 1
      ELSE 0
    END AS flag_material_abs_1,

    CASE
      WHEN prev_monthly_rate IS NULL OR prev_monthly_rate = 0 THEN 0
      WHEN ABS(SAFE_DIVIDE((MonthlyRate - prev_monthly_rate), prev_monthly_rate)) >= 0.10 THEN 1
      ELSE 0
    END AS flag_material_pct_10
  FROM ordered
)

SELECT *
FROM revisions
WHERE prev_snapshot_date IS NOT NULL;  -- only rows that actually have something to compare
