--- Purpose
-- Creates a persistent snapshot table for RESP-NET COVID-19 data
-- Stores point-in-time versions of the dataset to enable revision tracking
-- Partitions data by snapshot date for efficient time-based analysis
-- Clusters by state, month, age group, and rate type to optimize QA queries
-- *Bigquery limits clustering to 4 columns per table. I prioritized what I deemed to be most important for revision tracking


CREATE TABLE IF NOT EXISTS
`project2-482922.Covid_Rates.covid_snapshots` (
  snapshot_date DATE,
  snapshot_ts   TIMESTAMP,

  State STRING,
  Season STRING,
  _YearMonth INT64,
  month_date DATE,
  AgeCategory_Legend STRING,
  Sex_Label STRING,
  Race_Label STRING,
  Type STRING,

  MonthlyRate FLOAT64
)
PARTITION BY snapshot_date
CLUSTER BY State, month_date, AgeCategory_Legend, Type;
