-- =====================================================
-- McCain Foods Manufacturing Optimization Demo
-- SILVER & GOLD Transformations for mccain_ebc (Regenerated)
-- Catalog/Schema: mccain_ebc_catalog.ag_mfg
-- Notes:
-- - Updated to use percent-based USDA color caps in PSS (max_usda_color_0_pct..4_pct)
-- - Added new silver sources: silver_potato_variety_costs, silver_product_revenue_by_line, silver_current_product_demand
-- - Enriched silver_potato_load_quality with region and dm_band
-- - Gold cost exposure now leverages silver_potato_variety_costs for pricing context
-- - gold_cumulative_costs optionally joins revenue for margin context
-- =====================================================

-- =============================
-- SILVER TABLES (CLEANED DATA)
-- =============================

-- Silver: product specifications (PSS)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_product_specifications_pss AS
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
  -- updated percent-based USDA caps
  CAST(max_usda_color_0_pct AS DOUBLE) AS max_usda_color_0_pct,
  CAST(max_usda_color_1_pct AS DOUBLE) AS max_usda_color_1_pct,
  CAST(max_usda_color_2_pct AS DOUBLE) AS max_usda_color_2_pct,
  CAST(max_usda_color_3_pct AS DOUBLE) AS max_usda_color_3_pct,
  CAST(max_usda_color_4_pct AS DOUBLE) AS max_usda_color_4_pct,
  max_defect_points,
  CAST(min_dry_matter_pct AS DOUBLE) AS min_dry_matter_pct,
  CAST(max_dry_matter_pct AS DOUBLE) AS max_dry_matter_pct,
  approved_potato_varieties
FROM mccain_ebc_catalog.ag_mfg.raw_product_specifications_pss;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_product_specifications_pss SET TBLPROPERTIES ('comment' = 'PSS reference cleaned with standardized product ids/names. Includes percent-based USDA color caps (max_usda_color_0_pct..4_pct), dry matter thresholds, defect caps, and length grading by plant/line/SKU.');

-- Silver: potato variety costs (NEW)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_potato_variety_costs AS
SELECT
  effective_date,
  UPPER(TRIM(region)) AS region,
  supplier_id,
  INITCAP(TRIM(variety)) AS variety,
  TRIM(dm_band) AS dm_band,
  CAST(price_usd_per_ton AS DOUBLE) AS price_usd_per_ton,
  contract_id
FROM mccain_ebc_catalog.ag_mfg.raw_potato_variety_costs;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_potato_variety_costs SET TBLPROPERTIES ('comment' = 'Standardized potato variety cost list by effective_date, region, supplier, variety, and dry matter band. Used to enrich cost exposure and margin.');

-- Silver: product revenue by line (NEW)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_product_revenue_by_line AS
SELECT
  date,
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  COALESCE(plant_line, 'Unknown') AS plant_line,
  COALESCE(product_id, product_name) AS product_id_raw,
  COALESCE(product_name, product_id) AS product_name_raw,
  CAST(units_packed_kg AS DOUBLE) AS units_packed_kg,
  CAST(net_sales_usd AS DOUBLE) AS net_sales_usd,
  INITCAP(TRIM(channel)) AS channel,
  INITCAP(TRIM(tier)) AS tier,
  CASE WHEN plant_name LIKE 'NA-%' THEN 'NA' WHEN plant_name LIKE 'EU-%' THEN 'EU' ELSE NULL END AS region,
  CASE WHEN units_packed_kg > 0 THEN net_sales_usd / units_packed_kg ELSE NULL END AS price_per_kg,
  CAST(product_id_raw AS STRING) AS product_id,
  product_name_raw AS product_name
FROM mccain_ebc_catalog.ag_mfg.raw_product_revenue_by_line;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_product_revenue_by_line SET TBLPROPERTIES ('comment' = 'Daily revenue by plant line and SKU with standardized region and price_per_kg. Powers revenue and margin context.');

-- Silver: current product demand (NEW)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_current_product_demand AS
SELECT
  date,
  UPPER(TRIM(region)) AS region,
  INITCAP(TRIM(channel)) AS channel,
  COALESCE(product_id, product_name) AS product_id_raw,
  COALESCE(product_name, product_id) AS product_name_raw,
  CAST(orders_kg AS DOUBLE) AS orders_kg,
  CAST(forecast_kg AS DOUBLE) AS forecast_kg,
  CAST(promo_flag AS BOOLEAN) AS promo_flag,
  CAST(product_id_raw AS STRING) AS product_id,
  product_name_raw AS product_name
FROM mccain_ebc_catalog.ag_mfg.raw_current_product_demand;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_current_product_demand SET TBLPROPERTIES ('comment' = 'Demand signals (orders and forecast) per region/channel/product for planning and scenario alignment.');

-- Silver: potato load quality (enriched with region and dm_band)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_potato_load_quality AS
SELECT
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  CASE WHEN plant_name LIKE 'NA-%' THEN 'NA' WHEN plant_name LIKE 'EU-%' THEN 'EU' ELSE NULL END AS region,
  -- normalize variety label
  INITCAP(TRIM(COALESCE(VarietyLabel, 'Unknown'))) AS variety,
  load_number,
  -- derive arrival_date from load_number pattern '...-YYYYMMDD-...' else map into range deterministically
  CASE
    WHEN load_number RLIKE '.*-[0-9]{8}-.*' THEN to_date(regexp_extract(load_number, '.*-([0-9]{8})-.*', 1), 'yyyyMMdd')
    ELSE to_date('2025-07-10') + (ABS(HASH(load_number)) % 114)
  END AS arrival_date,
  CAST(effective_actual_weight AS DOUBLE) / 1000.0 AS tons,
  average_length_grading_mm,
  -- standardize percent fields to shares (0..1)
  COALESCE(usda_color_0_pct, 0.0) / 100.0 AS usda_color_0_share,
  COALESCE(usda_color_1_pct, 0.0) / 100.0 AS usda_color_1_share,
  COALESCE(usda_color_2_pct, 0.0) / 100.0 AS usda_color_2_share,
  COALESCE(usda_color_3_pct, 0.0) / 100.0 AS usda_color_3_share,
  COALESCE(usda_color_4_pct, 0.0) / 100.0 AS usda_color_4_share,
  total_defect_points,
  CAST(dry_matter_pct AS DOUBLE) AS dry_matter_pct,
  -- dm_band for pricing buckets
  CASE
    WHEN dry_matter_pct IS NULL THEN NULL
    WHEN dry_matter_pct <= 21.0 THEN '<=21%'
    WHEN dry_matter_pct > 21.0 AND dry_matter_pct <= 22.0 THEN '21-22%'
    WHEN dry_matter_pct > 22.0 AND dry_matter_pct < 23.0 THEN '22-23%'
    ELSE '>=23%'
  END AS dm_band
FROM mccain_ebc_catalog.ag_mfg.raw_potato_load_quality;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_potato_load_quality SET TBLPROPERTIES ('comment' = 'Raw potato loads standardized with region, normalized variety, arrival_date, tons, USDA color shares, dry_matter_pct and dm_band buckets for pricing.');

-- Silver: OSI PI line quality events
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_line_quality_events_osipi AS
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
    INITCAP(TRIM(COALESCE(variety, 'Unknown'))) AS variety,
    avg_length_mm,
    usda_color_0,
    usda_color_1,
    usda_color_2,
    usda_color_3,
    usda_color_4,
    total_defect_points,
    CAST(dry_solids_pct AS DOUBLE) AS dry_solids_pct
  FROM mccain_ebc_catalog.ag_mfg.raw_line_quality_events_osipi
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

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_line_quality_events_osipi SET TBLPROPERTIES ('comment' = 'OSI PI hourly line quality signals with date/hour/week_start helpers and fry color shares. dry_solids_pct proxies dry matter; anomalies appear in Aug-Sep windows per story.');

-- Silver: OEE production runs and downtime events
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_oee_production_runs_and_downtime_events AS
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
  TRY_CAST(end_time AS TIMESTAMP) AS end_time_ts,
  CAST(duration AS DOUBLE) / 60.0 AS duration_min,
  CAST(downtime_duration AS DOUBLE) / 60.0 AS downtime_min,
  qty_packed,
  num_totes_on
FROM mccain_ebc_catalog.ag_mfg.raw_oee_production_runs_and_downtime_events;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_oee_production_runs_and_downtime_events SET TBLPROPERTIES ('comment' = 'OEE runs and downtimes parsed with minutes and week_start. Supports OEE KPIs and event alignment.');

-- Silver: Line equipment inventory
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_line_equipment AS
SELECT
  plant_id,
  COALESCE(plant_name, 'Unknown') AS plant_name,
  COALESCE(plant_line, 'Unknown') AS plant_line,
  equipment_id,
  TRIM(LOWER(COALESCE(equipment_type, 'unknown'))) AS equipment_type,
  equipment_description,
  equipment_rated_throughput_tph,
  commission_date,
  CASE WHEN commission_date IS NOT NULL THEN ROUND(DATEDIFF(CURRENT_DATE(), commission_date) / 365.25, 2) ELSE NULL END AS equipment_age_years,
  CASE WHEN equipment_rated_throughput_tph IS NOT NULL THEN GREATEST(equipment_rated_throughput_tph * (1.0 - 0.005 * COALESCE(ROUND(DATEDIFF(CURRENT_DATE(), commission_date) / 365.25, 2), 0)), equipment_rated_throughput_tph * 0.70) ELSE NULL END AS effective_rated_throughput_tph
FROM mccain_ebc_catalog.ag_mfg.raw_line_equipment;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_line_equipment SET TBLPROPERTIES ('comment' = 'Line equipment inventory with normalized types and derived age/effective throughput.');

-- Silver helper: downtime events for dashboard table
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.silver_oee_events AS
SELECT
  CONCAT('EVT-', CAST(MONOTONICALLY_INCREASING_ID() AS STRING)) AS event_id,
  plant_name,
  plant_line,
  CAST(datetime AS TIMESTAMP) AS event_time,
  CAST(duration AS DOUBLE) / 60.0 AS duration_min,
  CAST(downtime_duration AS DOUBLE) / 60.0 AS downtime_min,
  TRIM(LOWER(COALESCE(downtime_cat_2, 'unknown'))) AS component,
  TRIM(LOWER(COALESCE(downtime_cat_1, 'unknown'))) AS code,
  CAST(NULL AS STRING) AS notes
FROM mccain_ebc_catalog.ag_mfg.raw_oee_production_runs_and_downtime_events;

ALTER TABLE mccain_ebc_catalog.ag_mfg.silver_oee_events SET TBLPROPERTIES ('comment' = 'Dashboard-ready downtime events with ids, plant/line, timestamps, durations, and normalized code/component fields.');

-- =============================
-- GOLD TABLES (AGGREGATIONS)
-- =============================

-- Gold: date spine for demo range
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_date_spine AS
SELECT d AS date,
       DAYOFWEEK(d) AS dow,
       CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
       DATE_TRUNC('WEEK', d) AS week_start
FROM (
  SELECT EXPLODE(SEQUENCE(to_date('2025-07-10'), to_date('2025-10-31'), INTERVAL 1 DAY)) AS d
) s;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_date_spine SET TBLPROPERTIES ('comment' = 'Daily date spine 2025-07-10..2025-10-31 with DOW, is_weekend, week_start.');

-- Gold: hourly line quality flagged vs PSS
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_line_hourly_quality AS
WITH pss AS (
  SELECT plant_id, plant_name, plant_line, product_name, min_dry_matter_pct, max_dry_matter_pct
  FROM mccain_ebc_catalog.ag_mfg.silver_product_specifications_pss
), src AS (
  SELECT
    lqe.datetime,
    lqe.date,
    lqe.hour,
    lqe.week_start,
    lqe.plant_id,
    lqe.plant_name,
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
  FROM mccain_ebc_catalog.ag_mfg.silver_line_quality_events_osipi lqe
  LEFT JOIN pss
    ON lqe.plant_id = pss.plant_id
   AND lqe.plant_name = pss.plant_name
   AND lqe.product_name = pss.product_name
)
SELECT
  *,
  CASE WHEN dry_solids_pct < (min_dry_matter_pct - 0.3) THEN TRUE ELSE FALSE END AS under_quality_flag,
  CASE WHEN dry_solids_pct > (max_dry_matter_pct + 0.3) THEN TRUE ELSE FALSE END AS over_quality_flag
FROM src;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_line_hourly_quality SET TBLPROPERTIES ('comment' = 'Hourly quality per plant_line/product vs PSS thresholds with 0.3pp tolerance flags.');

-- Gold: weekly quality by plant
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_quality_weekly AS
SELECT
  week_start,
  plant_name,
  AVG(dry_solids_pct) AS avg_dry_solids_pct,
  AVG(total_defect_points) AS avg_defect_points,
  CASE
    WHEN AVG(fry_color_0_share) >= GREATEST(AVG(fry_color_1_share), AVG(fry_color_2_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 0
    WHEN AVG(fry_color_1_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_2_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 1
    WHEN AVG(fry_color_2_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_1_share), AVG(fry_color_3_share), AVG(fry_color_4_share)) THEN 2
    WHEN AVG(fry_color_3_share) >= GREATEST(AVG(fry_color_0_share), AVG(fry_color_1_share), AVG(fry_color_2_share), AVG(fry_color_4_share)) THEN 3
    ELSE 4
  END AS fry_color_class_mode,
  COUNT_IF(under_quality_flag) AS hours_under_quality,
  COUNT_IF(over_quality_flag) AS hours_over_quality
FROM mccain_ebc_catalog.ag_mfg.gold_line_hourly_quality
GROUP BY week_start, plant_name;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_quality_weekly SET TBLPROPERTIES ('comment' = 'Weekly plant-level aggregates of dry solids, defect points, fry color class mode, and breach hours.');

-- Gold: OEE KPIs (daily by plant/line)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_oee_kpis AS
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
  FROM mccain_ebc_catalog.ag_mfg.silver_oee_production_runs_and_downtime_events
  GROUP BY date, plant_name, plant_line
)
SELECT
  *,
  CASE WHEN run_duration_min > 0 THEN qty_packed_kg / run_duration_min ELSE NULL END AS oee_performance
FROM agg;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_oee_kpis SET TBLPROPERTIES ('comment' = 'Daily OEE KPIs per plant/line: downtime, run minutes, qty packed, totes on, and performance proxy (kg/min).');

-- Gold: cost exposure (daily by plant/product) using variety costs
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_cost_exposure AS
WITH loads AS (
  SELECT
    plq.arrival_date AS date,
    plq.plant_name,
    plq.region,
    plq.variety,
    plq.dm_band,
    plq.tons,
    plq.dry_matter_pct
  FROM mccain_ebc_catalog.ag_mfg.silver_potato_load_quality plq
), price_map AS (
  SELECT
    effective_date,
    region,
    variety,
    dm_band,
    price_usd_per_ton,
    LEAD(effective_date, 1, to_date('2025-12-31')) OVER (PARTITION BY region, variety, dm_band ORDER BY effective_date) AS next_effective_date
  FROM mccain_ebc_catalog.ag_mfg.silver_potato_variety_costs
), loads_priced AS (
  SELECT
    l.date,
    l.plant_name,
    l.region,
    l.variety,
    l.dm_band,
    l.tons,
    l.dry_matter_pct,
    p.price_usd_per_ton
  FROM loads l
  LEFT JOIN price_map p
    ON l.region = p.region AND l.variety = p.variety AND l.dm_band = p.dm_band
   AND l.date >= p.effective_date AND l.date < p.next_effective_date
), thresholds AS (
  SELECT DISTINCT plant_name, product_name, min_dry_matter_pct, max_dry_matter_pct
  FROM mccain_ebc_catalog.ag_mfg.silver_product_specifications_pss
), regional_params AS (
  SELECT 'NA' AS region, 25.0 AS k_region, 180.0 AS downgrade_loss_per_ton UNION ALL
  SELECT 'EU' AS region, 28.0 AS k_region, 170.0 AS downgrade_loss_per_ton
)
SELECT
  lp.date,
  lp.plant_name,
  lp.region,
  t.product_name,
  SUM(lp.tons) AS tons_processed,
  AVG(lp.dry_matter_pct) AS avg_dry_matter_pct,
  -- raw spend from price table when available, else fallback to slope proxy
  SUM(COALESCE(lp.price_usd_per_ton, rp.k_region * GREATEST(lp.dry_matter_pct - 21.0, 0)) * lp.tons) AS raw_potato_spend_usd,
  SUM(CASE WHEN lp.dry_matter_pct > t.max_dry_matter_pct THEN (lp.dry_matter_pct - t.max_dry_matter_pct) ELSE 0 END * rp.k_region * lp.tons) AS over_quality_waste_usd,
  SUM(CASE WHEN lp.dry_matter_pct < t.min_dry_matter_pct THEN (t.min_dry_matter_pct - lp.dry_matter_pct) ELSE 0 END * rp.downgrade_loss_per_ton * lp.tons) AS downgrade_rework_exposure_usd
FROM loads_priced lp
LEFT JOIN thresholds t ON lp.plant_name = t.plant_name
LEFT JOIN regional_params rp ON lp.region = rp.region
GROUP BY lp.date, lp.plant_name, lp.region, t.product_name;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_cost_exposure SET TBLPROPERTIES ('comment' = 'Daily cost exposure per plant/product using variety price by region/variety/dm_band. Includes raw_potato_spend_usd, over_quality_waste_usd (DM above max) and downgrade_rework_exposure_usd (DM below min).');

-- Gold: cumulative costs by region with optional revenue join for gross margin
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_cumulative_costs AS
WITH cost_daily AS (
  SELECT
    date,
    CASE WHEN plant_name LIKE 'NA-%' THEN 'NA' WHEN plant_name LIKE 'EU-%' THEN 'EU' ELSE NULL END AS region,
    SUM(raw_potato_spend_usd) AS spend_usd,
    SUM(over_quality_waste_usd) AS over_quality_waste_usd
  FROM mccain_ebc_catalog.ag_mfg.gold_cost_exposure
  GROUP BY date, CASE WHEN plant_name LIKE 'NA-%' THEN 'NA' WHEN plant_name LIKE 'EU-%' THEN 'EU' ELSE NULL END
), rev_daily AS (
  SELECT
    date,
    region,
    SUM(net_sales_usd) AS revenue_usd
  FROM mccain_ebc_catalog.ag_mfg.silver_product_revenue_by_line
  GROUP BY date, region
)
SELECT
  c.date,
  c.region,
  SUM(c.spend_usd) OVER (PARTITION BY c.region ORDER BY c.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend_usd,
  SUM(c.over_quality_waste_usd) OVER (PARTITION BY c.region ORDER BY c.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_over_quality_waste_usd,
  -- simple gross margin context when revenue available: cumulative revenue minus cumulative spend
  SUM(r.revenue_usd) OVER (PARTITION BY c.region ORDER BY c.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    - SUM(c.spend_usd) OVER (PARTITION BY c.region ORDER BY c.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_gross_margin_usd
FROM cost_daily c
LEFT JOIN rev_daily r ON c.date = r.date AND c.region = r.region;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_cumulative_costs SET TBLPROPERTIES ('comment' = 'Cumulative raw potato spend and over-quality waste by region. Includes optional cumulative_gross_margin_usd by joining daily revenue.');

-- Gold: daily line metrics for dashboard (aggregate hourly)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_line_hourly_metrics AS
SELECT
  date,
  plant_name,
  plant_line,
  product_name,
  AVG(dry_solids_pct) AS blend_dm_pct,
  AVG(total_defect_points) AS defect_points,
  SUM(CASE WHEN under_quality_flag THEN 1 ELSE 0 END) AS hours_under_quality,
  SUM(CASE WHEN over_quality_flag THEN 1 ELSE 0 END) AS hours_over_quality
FROM mccain_ebc_catalog.ag_mfg.gold_line_hourly_quality
GROUP BY date, plant_name, plant_line, product_name;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_line_hourly_metrics SET TBLPROPERTIES ('comment' = 'Daily aggregates of hourly quality by plant/line/product: avg blend DM, defect points, and hours flagged. Powers daily line chart.');

-- Gold: change alignment between quality and OEE by date/hour
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_change_alignment AS
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
FROM mccain_ebc_catalog.ag_mfg.gold_line_hourly_quality q
LEFT JOIN mccain_ebc_catalog.ag_mfg.silver_oee_production_runs_and_downtime_events o
  ON q.date = o.date AND q.plant_name = o.plant_name AND q.plant_line = o.plant_line;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_change_alignment SET TBLPROPERTIES ('comment' = 'Alignment of quality flags with OEE events to trace correlations (e.g., NA-ID-P01 L3 cutter maintenance).');

-- Gold: KPI counters (single-row tables)

-- Total Tons Processed (Last 30 Days) using OEE qty (kg -> tons)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_counter_total_tons_last_30d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM mccain_ebc_catalog.ag_mfg.gold_oee_kpis),
base AS (
  SELECT * FROM mccain_ebc_catalog.ag_mfg.gold_oee_kpis
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(qty_packed_kg) / 1000.0 AS total_tons_last_30d FROM base;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_counter_total_tons_last_30d SET TBLPROPERTIES ('comment' = 'KPI: total tons processed over trailing 30 days.');

-- Raw Potato Spend (Last 30 Days)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_counter_raw_potato_spend_last_30d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM mccain_ebc_catalog.ag_mfg.gold_cost_exposure),
base AS (
  SELECT * FROM mccain_ebc_catalog.ag_mfg.gold_cost_exposure
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(raw_potato_spend_usd) AS raw_potato_spend_last_30d_usd FROM base;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_counter_raw_potato_spend_last_30d SET TBLPROPERTIES ('comment' = 'KPI: Sum of raw potato spend USD over trailing 30 days from gold_cost_exposure.');

-- OEE Performance (Avg Last 7 Days)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_counter_oee_performance_avg_last_7d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM mccain_ebc_catalog.ag_mfg.gold_oee_kpis),
base AS (
  SELECT * FROM mccain_ebc_catalog.ag_mfg.gold_oee_kpis
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 7) AND (SELECT max_date FROM anchor)
)
SELECT AVG(oee_performance) AS oee_performance_avg_last_7d FROM base;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_counter_oee_performance_avg_last_7d SET TBLPROPERTIES ('comment' = 'KPI: Average OEE performance over trailing 7 days.');

-- Downgrade/Rework Cost Exposure (Last 30 Days)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_counter_downgrade_rework_last_30d AS
WITH anchor AS (SELECT MAX(date) AS max_date FROM mccain_ebc_catalog.ag_mfg.gold_cost_exposure),
base AS (
  SELECT * FROM mccain_ebc_catalog.ag_mfg.gold_cost_exposure
  WHERE date BETWEEN date_sub((SELECT max_date FROM anchor), 30) AND (SELECT max_date FROM anchor)
)
SELECT SUM(downgrade_rework_exposure_usd) AS downgrade_rework_exposure_last_30d_usd FROM base;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_counter_downgrade_rework_last_30d SET TBLPROPERTIES ('comment' = 'KPI: Aggregated downgrade/rework exposure USD over trailing 30 days.');

-- Scenario recommendations (demo structure)
CREATE OR REPLACE TABLE mccain_ebc_catalog.ag_mfg.gold_scenario_recommendations AS
SELECT
  CONCAT('REC-', CAST(MONOTONICALLY_INCREASING_ID() AS STRING)) AS recommendation_id,
  CASE WHEN equipment_type = 'cutter' THEN 'equipment_upgrade' ELSE 'load_reallocation' END AS type,
  plant_name,
  plant_line,
  ROUND(GREATEST(COALESCE(equipment_rated_throughput_tph - effective_rated_throughput_tph, 0), 0) * 10000, 0) AS expected_savings_usd,
  CAST(0.2 AS DECIMAL(2,1)) AS expected_quality_delta_pp,
  CAST(0.6 AS DECIMAL(2,1)) AS confidence,
  CONCAT('Based on equipment age and throughput vs rated for ', equipment_id) AS rationale
FROM mccain_ebc_catalog.ag_mfg.silver_line_equipment;

ALTER TABLE mccain_ebc_catalog.ag_mfg.gold_scenario_recommendations SET TBLPROPERTIES ('comment' = 'Scenario suggestions combining equipment constraints; structure supports dashboard table fields.');
