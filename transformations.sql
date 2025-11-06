-- =====================================================
-- McCain Foods Manufacturing Optimization Demo
-- SILVER & GOLD Transformations for mccain_ebc
-- Catalog/Schema: demo_generator.brian_lui_mccain_ebc
-- =====================================================

-- =============================
-- SILVER TABLES (CLEANED DATA)
-- =============================

-- Silver: product specifications (PSS)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_product_specifications_pss AS
SELECT
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  COALESCE(plant_line, 'Unknown') AS plant_line,
  CAST(product_id AS STRING) AS product_id,
  COALESCE(product_name, 'Unknown') AS product_name,
  average_length_grading_mm_min,
  average_length_grading_mm_target,
  average_length_grading_mm_max,
  pct_min_50mm_length,
  pct_min_75mm_length,
  max_usda_color_0,
  max_usda_color_1,
  max_usda_color_2,
  max_usda_color_3,
  max_usda_color_4,
  max_defect_points,
  min_dry_matter_pct,
  max_dry_matter_pct,
  approved_potato_varieties
FROM demo_generator.brian_lui_mccain_ebc.raw_product_specifications_pss;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_product_specifications_pss SET TBLPROPERTIES ('comment' = 'PSS reference cleaned with standardized product ids/names. Contains per plant/line SKU thresholds for dry matter %, defect caps, USDA color distributions and length grading. Supports classification of over-/under-quality in gold layer.');

-- Silver: potato load quality
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_potato_load_quality AS
SELECT
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  TRIM(LOWER(COALESCE(VarietyLabel, 'unknown'))) AS variety,
  load_number,
  -- derive arrival_date from load_number pattern 'PLANT-YYYYMMDD-xxxx' when present; else map evenly into demo date range
  CASE
    WHEN load_number RLIKE '.*-[0-9]{8}-.*' THEN to_date(regexp_extract(load_number, '.*-([0-9]{8})-.*', 1), 'yyyyMMdd')
    ELSE to_date('2025-07-10') + (ABS(HASH(load_number)) % 114)
  END AS arrival_date,
  CAST(effective_actual_weight AS DOUBLE) / 1000.0 AS tons,
  average_length_grading_mm,
  COALESCE(usda_color_0_pct, 0.0) / 100.0 AS usda_color_0_share,
  COALESCE(usda_color_1_pct, 0.0) / 100.0 AS usda_color_1_share,
  COALESCE(usda_color_2_pct, 0.0) / 100.0 AS usda_color_2_share,
  COALESCE(usda_color_3_pct, 0.0) / 100.0 AS usda_color_3_share,
  COALESCE(usda_color_4_pct, 0.0) / 100.0 AS usda_color_4_share,
  total_defect_points,
  dry_matter_pct
FROM demo_generator.brian_lui_mccain_ebc.raw_potato_load_quality;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_potato_load_quality SET TBLPROPERTIES ('comment' = 'Raw potato incoming load quality per plant with normalized variety, tons (from kg), USDA color shares standardized to 0..1, defect points, dry matter %. arrival_date is left null (demo) and aligned via plant-level joins where needed.');

-- Silver: OSI PI line quality events
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_line_quality_events_osipi AS
WITH base AS (
  SELECT
    datetime,
    CAST(datetime AS DATE) AS date,
    HOUR(datetime) AS hour,
    DATE_TRUNC('WEEK', datetime) AS week_start,
    plant_id,
    COALESCE(plant_name, 'Unknown') AS plant_name,
    COALESCE(product_id, product_name) AS product_id_raw,
    COALESCE(product_name, product_id) AS product_name_raw,
    TRIM(LOWER(COALESCE(variety, 'unknown'))) AS variety,
    avg_length_mm,
    usda_color_0,
    usda_color_1,
    usda_color_2,
    usda_color_3,
    usda_color_4,
    total_defect_points,
    dry_solids_pct
  FROM demo_generator.brian_lui_mccain_ebc.raw_line_quality_events_osipi
), shares AS (
  SELECT
    *,
    CASE WHEN (usda_color_0 + usda_color_1 + usda_color_2 + usda_color_3 + usda_color_4) = 0 THEN 1 ELSE (usda_color_0 + usda_color_1 + usda_color_2 + usda_color_3 + usda_color_4) END AS total_fries
  FROM base
)
SELECT
  datetime,
  date,
  hour,
  week_start,
  plant_id,
  plant_name,
  -- standardize product id/name: prefer string id; if numeric only, cast to string
  CAST(product_id_raw AS STRING) AS product_id,
  product_name_raw AS product_name,
  variety,
  avg_length_mm,
  usda_color_0,
  usda_color_1,
  usda_color_2,
  usda_color_3,
  usda_color_4,
  total_defect_points,
  dry_solids_pct,
  -- fry color shares
  (usda_color_0 / total_fries) AS fry_color_0_share,
  (usda_color_1 / total_fries) AS fry_color_1_share,
  (usda_color_2 / total_fries) AS fry_color_2_share,
  (usda_color_3 / total_fries) AS fry_color_3_share,
  (usda_color_4 / total_fries) AS fry_color_4_share
FROM shares;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_line_quality_events_osipi SET TBLPROPERTIES ('comment' = 'OSI PI hourly line quality signals with date/hour/week_start helpers and fry color distribution shares from USDA counts. dry_solids_pct is a proxy for dry matter. Supports anomaly detection at line granularity.');

-- Silver: OEE production runs and downtime events
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_oee_production_runs_and_downtime_events AS
SELECT
  datetime,
  CAST(datetime AS DATE) AS date,
  DATE_TRUNC('WEEK', datetime) AS week_start,
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  COALESCE(plant_line, 'Unknown') AS plant_line,
  TRIM(LOWER(COALESCE(downtime_cat_1, 'unknown'))) AS downtime_cat_1,
  TRIM(LOWER(COALESCE(downtime_cat_2, 'unknown'))) AS downtime_cat_2,
  COALESCE(product_name, 'Unknown') AS product_name,
  start_time,
  -- parse end_time string to timestamp with safe cast
  TRY_CAST(end_time AS TIMESTAMP) AS end_time_ts,
  CAST(duration AS DOUBLE) / 60.0 AS duration_min,
  CAST(downtime_duration AS DOUBLE) / 60.0 AS downtime_min,
  qty_packed,
  num_totes_on
FROM demo_generator.brian_lui_mccain_ebc.raw_oee_production_runs_and_downtime_events;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_oee_production_runs_and_downtime_events SET TBLPROPERTIES ('comment' = 'OEE runs and downtime events with parsed end_time, duration_min and downtime_min, plus week_start. Used to compute OEE KPIs and correlate downtime codes with quality anomalies.');

-- Silver: Line equipment inventory
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_line_equipment AS
SELECT
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  COALESCE(plant_line, 'Unknown') AS plant_line,
  equipment_id,
  TRIM(LOWER(COALESCE(equipment_type, 'unknown'))) AS equipment_type,
  equipment_description,
  equipment_rated_throughput_tph,
  commission_date,
  -- simple age in years
  CASE WHEN commission_date IS NOT NULL THEN ROUND(DATEDIFF(CURRENT_DATE(), commission_date) / 365.25, 2) ELSE NULL END AS equipment_age_years,
  -- effective rated throughput: small decay with age (-0.5% per year, floor 70% of rated)
  CASE WHEN equipment_rated_throughput_tph IS NOT NULL THEN GREATEST(equipment_rated_throughput_tph * (1.0 - 0.005 * COALESCE(ROUND(DATEDIFF(CURRENT_DATE(), commission_date) / 365.25, 2), 0)), equipment_rated_throughput_tph * 0.70) ELSE NULL END AS effective_rated_throughput_tph
FROM demo_generator.brian_lui_mccain_ebc.raw_line_equipment;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_line_equipment SET TBLPROPERTIES ('comment' = 'Line equipment inventory with normalized types and derived age/effective throughput. Supports scenario planning for equipment upgrades, throughput constraints, and OEE analysis.');


-- =============================
-- GOLD TABLES (AGGREGATIONS)
-- =============================

-- Gold: date spine for demo range
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_date_spine AS
SELECT d AS date,
       DAYOFWEEK(d) AS dow,
       CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
       DATE_TRUNC('WEEK', d) AS week_start
FROM (
  SELECT EXPLODE(SEQUENCE(to_date('2025-07-10'), to_date('2025-10-31'), INTERVAL 1 DAY)) AS d
) s;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_date_spine SET TBLPROPERTIES ('comment' = 'Daily date spine from 2025-07-10..2025-10-31 with DOW, is_weekend, and week_start. Used for consistent bucketing and joins.');

-- Gold: hourly line quality flagged vs PSS
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_line_hourly_quality AS
WITH pss AS (
  SELECT plant_id, plant_name, plant_line, product_name, min_dry_matter_pct, max_dry_matter_pct
  FROM demo_generator.brian_lui_mccain_ebc.silver_product_specifications_pss
), src AS (
  SELECT
    lqe.datetime,
    lqe.date,
    lqe.hour,
    lqe.week_start,
    lqe.plant_id,
    lqe.plant_name,
    -- derive line_id from product specs join (some OSI PI lacks line field); fallback to 'Unknown'
    COALESCE(pss.plant_line, 'Unknown') AS plant_line,
    lqe.product_name,
    lqe.variety,
    lqe.avg_length_mm,
    lqe.usda_color_0,
    lqe.usda_color_1,
    lqe.usda_color_2,
    lqe.usda_color_3,
    lqe.usda_color_4,
    lqe.total_defect_points,
    lqe.dry_solids_pct,
    lqe.fry_color_0_share,
    lqe.fry_color_1_share,
    lqe.fry_color_2_share,
    lqe.fry_color_3_share,
    lqe.fry_color_4_share,
    pss.min_dry_matter_pct,
    pss.max_dry_matter_pct
  FROM demo_generator.brian_lui_mccain_ebc.silver_line_quality_events_osipi lqe
  LEFT JOIN pss
    ON lqe.plant_id = pss.plant_id
   AND lqe.plant_name = pss.plant_name
   AND lqe.product_name = pss.product_name
)
SELECT
  *,
  -- tolerance 0.3pp
  CASE WHEN dry_solids_pct < (min_dry_matter_pct - 0.3) THEN TRUE ELSE FALSE END AS under_quality_flag,
  CASE WHEN dry_solids_pct > (max_dry_matter_pct + 0.3) THEN TRUE ELSE FALSE END AS over_quality_flag
FROM src;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_line_hourly_quality SET TBLPROPERTIES ('comment' = 'Hourly quality per plant_line/product vs PSS thresholds. Includes under_quality_flag and over_quality_flag using 0.3pp tolerance as per story. Enables anomaly windows to be detected and quantified.');

-- Gold: weekly quality by plant
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_quality_weekly AS
SELECT
  week_start,
  plant_name,
  AVG(dry_solids_pct) AS avg_dry_solids_pct,
  AVG(total_defect_points) AS avg_defect_points,
  -- simple mode approximation: bucket by highest avg share
  CASE
    WHEN AVG(fry_color_0_share) >= GREATEST(AVG(fry_color_1_share), AVG(fry_color_2_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 0
    WHEN AVG(fry_color_1_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_2_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 1
    WHEN AVG(fry_color_2_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_1_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 2
    WHEN AVG(fry_color_3_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_1_share), AVG(fry_color_2_share), AVG(fry_color_4_share)) THEN 3
    ELSE 4
  END AS fry_color_class_mode,
  COUNT_IF(under_quality_flag) AS hours_under_quality,
  COUNT_IF(over_quality_flag) AS hours_over_quality
FROM demo_generator.brian_lui_mccain_ebc.gold_line_hourly_quality
GROUP BY week_start, plant_name;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_quality_weekly SET TBLPROPERTIES ('comment' = 'Weekly plant-level aggregates of dry solids, defect points, fry color class mode, and counts of hours in under/over-quality breach. Powers bar chart on weekly defects by plant.');

-- Gold: OEE KPIs (daily by plant/line)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_oee_kpis AS
WITH agg AS (
  SELECT
    date,
    plant_name,
    plant_line,
    COUNT(*) AS event_count,
    SUM(downtime_min) AS total_downtime_min,
    SUM(duration_min) AS run_duration_min,
    SUM(qty_packed) AS qty_packed_kg,
    SUM(num_totes_on) AS num_totes_on
  FROM demo_generator.brian_lui_mccain_ebc.silver_oee_production_runs_and_downtime_events
  GROUP BY date, plant_name, plant_line
)
SELECT
  *,
  -- performance proxy: qty_packed per run minute normalized
  CASE WHEN run_duration_min > 0 THEN qty_packed_kg / run_duration_min ELSE NULL END AS oee_performance
FROM agg;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_oee_kpis SET TBLPROPERTIES ('comment' = 'Daily OEE KPIs per plant/line: downtime minutes, run duration, qty packed, totes on, event counts, and performance proxy (qty/min). Supports counters and OEE lines visualization.');

-- Gold: cost exposure (daily by plant/product)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_cost_exposure AS
WITH region_params AS (
  -- region from plant_name prefix: NA-*, EU-*
  SELECT 'NA' AS region, 25.0 AS k_region, 180.0 AS rework_factor UNION ALL
  SELECT 'EU' AS region, 28.0 AS k_region, 170.0 AS rework_factor
), loads AS (
  SELECT
    plq.plant_name,
    CASE WHEN plq.plant_name LIKE 'NA-%' THEN 'NA' WHEN plq.plant_name LIKE 'EU-%' THEN 'EU' ELSE 'NA' END AS region,
    CAST(NULL AS DATE) AS date,
    plq.variety,
    plq.tons,
    plq.dry_matter_pct
  FROM demo_generator.brian_lui_mccain_ebc.silver_potato_load_quality plq
), thresholds AS (
  SELECT DISTINCT plant_name, product_name, min_dry_matter_pct, max_dry_matter_pct
  FROM demo_generator.brian_lui_mccain_ebc.silver_product_specifications_pss
)
SELECT
  l.plant_name,
  l.region,
  t.product_name,
  l.date,
  SUM(l.tons) AS tons_processed,
  -- blended cost per ton proxy: base + k*(DM-21%) but we only compute incremental against 21
  AVG(l.dry_matter_pct) AS avg_dry_matter_pct,
  SUM(CASE WHEN l.dry_matter_pct > t.max_dry_matter_pct THEN (l.dry_matter_pct - t.max_dry_matter_pct) ELSE 0 END * rp.k_region * l.tons) AS over_quality_waste_usd,
  SUM(CASE WHEN l.dry_matter_pct < t.min_dry_matter_pct THEN (t.min_dry_matter_pct - l.dry_matter_pct) ELSE 0 END * rp.rework_factor * l.tons) AS downgrade_rework_exposure_usd,
  -- raw potato spend approximation: base spend proportional to DM and tons
  SUM((l.dry_matter_pct - 21.0) * rp.k_region * l.tons) AS raw_potato_spend_usd
FROM loads l
LEFT JOIN thresholds t ON l.plant_name = t.plant_name
LEFT JOIN region_params rp ON l.region = rp.region
GROUP BY l.plant_name, l.region, t.product_name, l.date;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_cost_exposure SET TBLPROPERTIES ('comment' = 'Daily cost exposure per plant/product using load DM vs PSS thresholds. over_quality_waste_usd for DM above max; downgrade_rework_exposure_usd for DM below min; raw_potato_spend_usd approx via k_region*(DM-21)*tons. Drives spend counters and exposure metrics.');

-- Gold: cumulative costs by region
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_cumulative_costs AS
WITH base AS (
  SELECT
    CASE WHEN plant_name LIKE 'NA-%' THEN 'NA' WHEN plant_name LIKE 'EU-%' THEN 'EU' ELSE 'NA' END AS region,
    COALESCE(date, to_date('2025-07-10')) AS date,
    raw_potato_spend_usd,
    over_quality_waste_usd
  FROM demo_generator.brian_lui_mccain_ebc.gold_cost_exposure
), agg AS (
  SELECT date, region,
         SUM(raw_potato_spend_usd) AS spend_usd,
         SUM(over_quality_waste_usd) AS over_quality_waste_usd
  FROM base
  GROUP BY date, region
)
SELECT
  a.date,
  a.region,
  SUM(a.spend_usd) OVER (PARTITION BY a.region ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend_usd,
  SUM(a.over_quality_waste_usd) OVER (PARTITION BY a.region ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_over_quality_waste_usd
FROM agg a;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_cumulative_costs SET TBLPROPERTIES ('comment' = 'Cumulative raw potato spend and over-quality waste by region over time. Supports stacked bars and overlays to emphasize EU-NL-P03 contribution.');

-- Gold: line hourly metrics (daily aggregated for dashboard)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_line_hourly_metrics AS
SELECT
  date,
  plant_name,
  plant_line,
  product_name,
  AVG(dry_solids_pct) AS blend_dm_pct,
  AVG(total_defect_points) AS defect_points,
  SUM(CASE WHEN under_quality_flag THEN 1 ELSE 0 END) AS hours_under_quality,
  SUM(CASE WHEN over_quality_flag THEN 1 ELSE 0 END) AS hours_over_quality
FROM demo_generator.brian_lui_mccain_ebc.gold_line_hourly_quality
GROUP BY date, plant_name, plant_line, product_name;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_line_hourly_metrics SET TBLPROPERTIES ('comment' = 'Daily aggregates of hourly quality by plant/line/product: avg blend DM pct, defect points, and hours flagged. Powers daily line chart for blend intake DM vs target.');

-- Gold: change alignment between quality and OEE by date/hour
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_change_alignment AS
SELECT
  q.date,
  q.hour,
  q.plant_name,
  q.plant_line,
  q.product_name,
  q.dry_solids_pct,
  q.under_quality_flag,
  q.over_quality_flag,
  o.downtime_cat_1,
  o.downtime_cat_2,
  o.duration_min,
  o.downtime_min
FROM demo_generator.brian_lui_mccain_ebc.gold_line_hourly_quality q
LEFT JOIN demo_generator.brian_lui_mccain_ebc.silver_oee_production_runs_and_downtime_events o
  ON q.date = o.date AND q.plant_name = o.plant_name AND q.plant_line = o.plant_line;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_change_alignment SET TBLPROPERTIES ('comment' = 'Alignment table joining hourly quality to OEE events by date/plant/line to expose correlation with downtime categories during anomaly windows (e.g., NA-ID-P01 L3 cutter-related events).');

-- Gold: downtime events table passthrough for dashboard
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.silver_oee_events AS
SELECT
  CONCAT('EVT-', CAST(MONOTONICALLY_INCREASING_ID() AS STRING)) AS event_id,
  plant_name,
  plant_line,
  CAST(datetime AS TIMESTAMP) AS event_time,
  duration_min,
  downtime_min,
  downtime_cat_2 AS component,
  downtime_cat_1 AS code,
  CAST(NULL AS STRING) AS notes
FROM demo_generator.brian_lui_mccain_ebc.silver_oee_production_runs_and_downtime_events;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.silver_oee_events SET TBLPROPERTIES ('comment' = 'Detail table for downtime events aligned to dashboard needs: event_id, plant_id/name/line, time, duration, code/component. Used to trace specific codes like DOW-PI-4521 and DOW-PI-4574.');

-- Gold: KPI counters (single row)
-- Total tons processed (last 30 days) from gold_line_hourly_metrics approximated by OEE qty (convert kg to tons)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_total_tons_last_30d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM demo_generator.brian_lui_mccain_ebc.gold_oee_kpis),
base AS (
  SELECT * FROM demo_generator.brian_lui_mccain_ebc.gold_oee_kpis
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(qty_packed_kg) / 1000.0 AS total_tons_last_30d FROM base;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_total_tons_last_30d SET TBLPROPERTIES ('comment' = 'Single-row KPI: total tons processed over trailing 30 days using qty_packed_kg from OEE KPIs.');

-- Raw potato spend (last 30 days)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_raw_potato_spend_last_30d AS
WITH anchor AS (SELECT MAX(COALESCE(date, to_date('2025-07-10'))) AS max_date FROM demo_generator.brian_lui_mccain_ebc.gold_cost_exposure),
base AS (
  SELECT * FROM demo_generator.brian_lui_mccain_ebc.gold_cost_exposure
  WHERE COALESCE(date, to_date('2025-07-10')) BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(raw_potato_spend_usd) AS raw_potato_spend_last_30d_usd FROM base;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_raw_potato_spend_last_30d SET TBLPROPERTIES ('comment' = 'Single-row KPI: Sum of raw potato spend USD over trailing 30 days from gold_cost_exposure.');

-- OEE performance (avg last 7 days)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_oee_performance_avg_last_7d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM demo_generator.brian_lui_mccain_ebc.gold_oee_kpis),
base AS (
  SELECT * FROM demo_generator.brian_lui_mccain_ebc.gold_oee_kpis
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 7) AND (SELECT max_date FROM anchor)
)
SELECT AVG(oee_performance) AS oee_performance_avg_last_7d FROM base;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_oee_performance_avg_last_7d SET TBLPROPERTIES ('comment' = 'Single-row KPI: Average OEE performance over trailing 7 days.');

-- Downgrade/Rework cost exposure (last 30 days)
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_downgrade_rework_last_30d AS
WITH anchor AS (SELECT MAX(COALESCE(date, to_date('2025-07-10'))) AS max_date FROM demo_generator.brian_lui_mccain_ebc.gold_cost_exposure),
base AS (
  SELECT * FROM demo_generator.brian_lui_mccain_ebc.gold_cost_exposure
  WHERE COALESCE(date, to_date('2025-07-10')) BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(downgrade_rework_exposure_usd) AS downgrade_rework_exposure_last_30d_usd FROM base;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_counter_downgrade_rework_last_30d SET TBLPROPERTIES ('comment' = 'Single-row KPI: Aggregated downgrade/rework exposure USD over trailing 30 days.');

-- NOTE: Scenario recommendations gold table would normally combine manuals/specs with OEE and cost impacts. For this demo, we stub a structure for dashboard table.
CREATE OR REPLACE TABLE demo_generator.brian_lui_mccain_ebc.gold_scenario_recommendations AS
SELECT
  CONCAT('REC-', CAST(MONOTONICALLY_INCREASING_ID() AS STRING)) AS recommendation_id,
  CASE WHEN equipment_type = 'cutter' THEN 'equipment_upgrade' ELSE 'load_reallocation' END AS type,
  plant_name,
  plant_line,
  ROUND(GREATEST(COALESCE(equipment_rated_throughput_tph - effective_rated_throughput_tph, 0), 0) * 10000, 0) AS expected_savings_usd,
  CAST(0.2 AS DECIMAL(2,1)) AS expected_quality_delta_pp,
  CAST(0.6 AS DECIMAL(2,1)) AS confidence,
  CONCAT('Based on equipment age and throughput vs rated for ', equipment_id) AS rationale
FROM demo_generator.brian_lui_mccain_ebc.silver_line_equipment;

ALTER TABLE demo_generator.brian_lui_mccain_ebc.gold_scenario_recommendations SET TBLPROPERTIES ('comment' = 'Demo scenario suggestions combining equipment constraints; structure supports dashboard table with savings, quality delta, confidence, and rationale.');
