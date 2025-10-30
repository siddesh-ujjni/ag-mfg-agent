-- =============================================================
-- mccain foods demo - silver and gold transformations (databricks sql)
-- project: mccain
-- author: sqlgenerationagent
-- purpose: rebuild silver (cleaned) and gold (business) layers from current RAW tables
-- to support closed-loop blend optimization, PSS comparisons, alerts, costs,
-- supplier reliability, and scenario-ready optimization as per demo story.
-- notes:
-- - uses only the raw tables defined in demo_story.json for this project.
-- - never creates views; always create or replace table.
-- - all statements terminated with semicolons.
-- - built for spark sql / databricks sql compatibility.
-- =============================================================

-- =============================
-- configuration: catalog/schema
-- =============================
USE CATALOG brlui;
USE SCHEMA brian_lui_mccain;

-- =====================================================
-- SILVER LAYER: CLEAN, STANDARDIZE, DERIVE FIELDS
-- =====================================================

-- 1) silver_potato_load_quality (from raw_potato_load_quality)
CREATE OR REPLACE TABLE silver_potato_load_quality AS
SELECT /*+ REPARTITION(plant_id) */
  load_id,
  LOWER(TRIM(plant_id)) AS plant_id,
  COALESCE(plant_name, INITCAP(plant_id)) AS plant_name,
  CAST(delivery_time AS TIMESTAMP) AS delivery_time,
  CAST(delivery_time AS DATE) AS date,
  HOUR(CAST(delivery_time AS TIMESTAMP)) AS hour_of_day,
  DATE_TRUNC('WEEK', CAST(delivery_time AS TIMESTAMP)) AS week_start,
  supplier_id,
  LOWER(TRIM(field_region)) AS supplier_region,
  LOWER(TRIM(variety)) AS variety,
  COALESCE(varietylabel, variety) AS variety_label_raw,
  loadnumber AS load_number,
  CAST(gross_weight_kg AS DOUBLE) AS gross_weight_kg,
  CAST(netweightonload AS DOUBLE) AS net_weight_kg,
  CAST(avg_length_mm AS DOUBLE) AS avg_length_mm,
  UPPER(TRIM(usda_color)) AS usda_color,
  CAST(defect_points AS DOUBLE) AS defect_points,
  CAST(dry_solids_pct AS DOUBLE) AS dry_solids_pct,
  attributecode,
  sampleattributelabel,
  CAST(attributevalue AS DOUBLE) AS attributevalue,
  LOWER(TRIM(intended_line_id)) AS intended_line_id,
  intended_sku_id AS intended_sku_id,
  COALESCE(storage_bin_id, 'UNSPEC') AS storage_bin_id,
  (dry_solids_pct < 19.5) AS dry_solids_low_flag,
  (defect_points > 2.2) AS defect_high_flag
FROM brlui.brian_lui_mccain.raw_potato_load_quality;
ALTER TABLE silver_potato_load_quality SET TBLPROPERTIES ('comment' = 'Cleaned per-load intake measures with derived date/hour/week and heuristic flags. Supports blend intake, supplier attribution, and delivery vs PSS checks.');

-- 2) silver_line_quality_events (from raw_line_quality_events_osipi)
CREATE OR REPLACE TABLE silver_line_quality_events AS
SELECT /*+ REPARTITION(plantid) */
  CAST(datetime AS TIMESTAMP) AS event_time,
  CAST(datetime AS DATE) AS date,
  DATE_TRUNC('HOUR', CAST(datetime AS TIMESTAMP)) AS hour_bucket,
  DATE_TRUNC('WEEK', CAST(datetime AS TIMESTAMP)) AS week_start,
  LOWER(TRIM(plantid)) AS plant_id,
  productid AS sku_id,
  LOWER(TRIM(variety)) AS variety,
  CAST(average_length_ml AS DOUBLE) AS avg_length_mm,
  CAST(usda_color_0 AS BIGINT) AS usda_color_0,
  CAST(usda_color_1 AS BIGINT) AS usda_color_1,
  CAST(usda_color_2 AS BIGINT) AS usda_color_2,
  CAST(usda_color_3 AS BIGINT) AS usda_color_3,
  CAST(usda_color_4 AS BIGINT) AS usda_color_4,
  CAST(total_defect_points AS DOUBLE) AS defect_points,
  CAST(dry_solids_pct AS DOUBLE) AS dry_solids_pct,
  CASE
    WHEN greatest(usda_color_0, usda_color_1, usda_color_2, usda_color_3, usda_color_4) = usda_color_0 THEN 'A'
    WHEN greatest(usda_color_0, usda_color_1, usda_color_2, usda_color_3, usda_color_4) = usda_color_1 THEN 'B'
    WHEN greatest(usda_color_0, usda_color_1, usda_color_2, usda_color_3, usda_color_4) = usda_color_2 THEN 'C'
    WHEN greatest(usda_color_0, usda_color_1, usda_color_2, usda_color_3, usda_color_4) = usda_color_3 THEN 'D'
    ELSE 'E'
  END AS usda_color_mode
FROM brlui.brian_lui_mccain.raw_line_quality_events_osipi;
ALTER TABLE silver_line_quality_events SET TBLPROPERTIES ('comment' = 'OSI PI line-level quality events normalized with hourly/week buckets and modal USDA color. Used to derive line-hour quality and alerts.');

-- 3) silver_oee_equipment_events (from raw_oee_equipment_events)
CREATE OR REPLACE TABLE silver_oee_equipment_events AS
SELECT /*+ REPARTITION(plant_id, line_cd) */
  CAST(time_id AS DATE) AS time_id,
  LOWER(TRIM(plant_id)) AS plant_id,
  LOWER(TRIM(line_cd)) AS line_id,
  plant_name,
  plant_line,
  primary_reason_cd,
  secondary_reason_cd,
  downtime_cat_1,
  downtime_cat_2,
  product AS sku_id,
  plant_line_product,
  workshift,
  start_time,
  CAST(start_hour AS INT) AS start_hour,
  end_time,
  CAST(end_hour AS INT) AS end_hour,
  CAST(duration AS INT) AS duration,
  CAST(downtime_duration AS INT) AS downtime_duration,
  CAST(qty_packed AS DOUBLE) AS qty_packed,
  CAST(num_totes_on AS DOUBLE) AS num_totes_on
FROM brlui.brian_lui_mccain.raw_oee_equipment_events;
ALTER TABLE silver_oee_equipment_events SET TBLPROPERTIES ('comment' = 'OEE/events cleaned and standardized for joins by plant/line/date-hour. Supports derate and performance context.');

-- 4) silver_hourly_blend_intake (approximate hourly blend sources and fractions from silver_potato_load_quality)
CREATE OR REPLACE TABLE silver_hourly_blend_intake AS
WITH loads AS (
  SELECT
    LOWER(TRIM(plant_id)) AS plant_id,
    LOWER(TRIM(intended_line_id)) AS line_id,
    intended_sku_id AS sku_id,
    DATE_TRUNC('HOUR', CAST(delivery_time AS TIMESTAMP)) AS hour_bucket,
    COALESCE(storage_bin_id, 'UNSPEC') AS source_id,
    AVG(dry_solids_pct) AS dry_solids_pct,
    AVG(defect_points) AS defect_points,
    AVG(avg_length_mm) AS avg_length_mm,
    ANY_VALUE(variety) AS variety,
    SUM(net_weight_kg) AS weight_kg
  FROM silver_potato_load_quality
  GROUP BY LOWER(TRIM(plant_id)), LOWER(TRIM(intended_line_id)), intended_sku_id, DATE_TRUNC('HOUR', CAST(delivery_time AS TIMESTAMP)), COALESCE(storage_bin_id, 'UNSPEC')
), frac AS (
  SELECT plant_id, line_id, sku_id, hour_bucket,
         COLLECT_LIST(source_id) AS sources,
         COLLECT_LIST(weight_kg) AS weights,
         COLLECT_LIST(variety) AS varieties,
         SUM(dry_solids_pct * weight_kg) / NULLIF(SUM(weight_kg),0) AS dry_solids_pct_blend,
         SUM(defect_points * weight_kg) / NULLIF(SUM(weight_kg),0) AS defect_points_blend,
         SUM(avg_length_mm * weight_kg) / NULLIF(SUM(weight_kg),0) AS avg_length_mm_blend,
         SUM(weight_kg) AS total_weight_kg
  FROM loads
  GROUP BY plant_id, line_id, sku_id, hour_bucket
)
SELECT plant_id, line_id, sku_id, hour_bucket, sources, varieties,
       transform(weights, w -> w / NULLIF(AGGREGATE(weights, CAST(0.0 AS DOUBLE), (acc, x) -> acc + CAST(x AS DOUBLE)), 0.0)) AS fractions,
       dry_solids_pct_blend, defect_points_blend, avg_length_mm_blend, total_weight_kg,
       CASE
         WHEN exists(transform(varieties, x -> lower(x)), v -> v LIKE '%russet burbank%') THEN 0.28
         WHEN exists(transform(varieties, x -> lower(x)), v -> v LIKE '%shepody%') THEN 0.25
         WHEN exists(transform(varieties, x -> lower(x)), v -> v LIKE '%russet norkotah%') THEN 0.23
         ELSE 0.24
       END AS potato_cost_per_kg_blend
FROM frac;
ALTER TABLE silver_hourly_blend_intake SET TBLPROPERTIES ('comment' = 'Line-hour blend approximation with arrays of sources and fractions and computed blended attributes and cost proxy. Drives hourly quality and optimizer.');

-- 5) silver_scenario_inputs: normalized scenario input structures supporting what-if cases (from available silver/raw tables)
CREATE OR REPLACE TABLE silver_scenario_inputs AS
WITH loads AS (
  SELECT load_id, plant_id, intended_line_id AS line_id, intended_sku_id AS sku_id,
         storage_bin_id, date, delivery_time,
         variety, supplier_region,
         usda_color, defect_points, dry_solids_pct,
         gross_weight_kg, net_weight_kg
  FROM silver_potato_load_quality
), bins AS (
  SELECT storage_bin_id AS bin_id,
         AVG(CASE WHEN variety LIKE '%burbank%' THEN 7.0 WHEN variety LIKE '%norkotah%' THEN 6.0 ELSE 6.5 END) AS bin_temp_c,
         COUNT(*) AS turnover_events
  FROM silver_potato_load_quality
  GROUP BY storage_bin_id
), equipment AS (
  SELECT DISTINCT plant_id, line_id, sku_id,
         0.06 AS expected_defect_reduction_rate,
         0.008 AS expected_oil_uptake_reduction_rate,
         250000.0 AS capex_usd,
         24 AS install_downtime_hours
  FROM silver_oee_equipment_events
)
SELECT
  -- stored load colour degradation inputs
  l.load_id,
  l.plant_id,
  l.line_id,
  l.sku_id,
  COALESCE(l.storage_bin_id, 'UNSPEC') AS storage_bin_id,
  l.date,
  l.delivery_time,
  l.variety,
  l.supplier_region,
  l.usda_color,
  l.defect_points,
  l.dry_solids_pct,
  l.gross_weight_kg,
  l.net_weight_kg,
  b.bin_id,
  COALESCE(b.bin_temp_c, 6.5) AS bin_temp_c,
  COALESCE(b.turnover_events, 0) AS bin_turnover_events,
  -- equipment upgrade effectiveness inputs
  e.expected_defect_reduction_rate,
  e.expected_oil_uptake_reduction_rate,
  e.capex_usd,
  e.install_downtime_hours
FROM loads l
LEFT JOIN bins b ON b.bin_id = l.storage_bin_id
LEFT JOIN equipment e ON e.plant_id = l.plant_id AND e.line_id = l.line_id AND e.sku_id = l.sku_id;
ALTER TABLE silver_scenario_inputs SET TBLPROPERTIES ('comment' = 'Scenario-ready input structures combining load/bin context and equipment assumptions to feed what-if models: storage colour degradation and equipment upgrade effectiveness.');

-- =====================================================
-- GOLD LAYER: BUSINESS AGGREGATIONS AND METRICS
-- =====================================================

-- Reference mapping for USDA color grade ordering
CREATE OR REPLACE TABLE gold_ref_usda_ordinal AS
SELECT 'A' AS usda_color, 0 AS ord UNION ALL SELECT 'B', 1 UNION ALL SELECT 'C', 2 UNION ALL SELECT 'D', 3 UNION ALL SELECT 'E', 4;
ALTER TABLE gold_ref_usda_ordinal SET TBLPROPERTIES ('comment' = 'Reference mapping for USDA color grade ordering (A light to E dark).');

-- a) gold_blend_hourly_quality: compare hourly blend vs PSS and compute compliance and alert bands
CREATE OR REPLACE TABLE gold_blend_hourly_quality AS
WITH pss AS (
  SELECT LOWER(TRIM(plantid)) AS plant_id, productid AS sku_id,
         average_length_grading_min,
         average_length_grading_target,
         average_length_grading_max,
         max_defect_points,
         min_dry_matter_pct,
         max_dry_matter_pct,
         max_usda_color_0, max_usda_color_1, max_usda_color_2, max_usda_color_3, max_usda_color_4,
         approved_potato_varieties
  FROM brlui.brian_lui_mccain.raw_product_specifications_pss
), base AS (
  SELECT h.plant_id, h.line_id, h.sku_id, h.hour_bucket,
         h.sources, h.fractions, h.varieties,
         h.dry_solids_pct_blend, h.defect_points_blend, h.avg_length_mm_blend,
         p.average_length_grading_min, p.average_length_grading_max,
         p.max_defect_points, p.min_dry_matter_pct, p.max_dry_matter_pct,
         p.approved_potato_varieties,
         h.potato_cost_per_kg_blend
  FROM silver_hourly_blend_intake h
  LEFT JOIN pss p ON p.plant_id = h.plant_id AND p.sku_id = h.sku_id
), flags AS (
  SELECT *,
    (dry_solids_pct_blend >= COALESCE(min_dry_matter_pct, dry_solids_pct_blend)) AS dry_ok,
    (defect_points_blend <= COALESCE(max_defect_points, defect_points_blend)) AS defect_ok,
    (avg_length_mm_blend BETWEEN COALESCE(average_length_grading_min, avg_length_mm_blend) AND COALESCE(average_length_grading_max, avg_length_mm_blend)) AS length_ok,
    CASE WHEN approved_potato_varieties IS NULL OR size(approved_potato_varieties) = 0 THEN TRUE
         ELSE size(array_intersect(transform(varieties, x -> lower(x)), transform(approved_potato_varieties, x -> lower(x)))) > 0 END AS variety_ok
  FROM base
)
SELECT
  plant_id,
  line_id,
  sku_id,
  hour_bucket,
  dry_solids_pct_blend,
  defect_points_blend,
  avg_length_mm_blend,
  COALESCE(min_dry_matter_pct, dry_solids_pct_blend) AS sku_min_dry_solids_pct,
  potato_cost_per_kg_blend,
  sources,
  fractions,
  (dry_ok AND defect_ok AND length_ok AND variety_ok) AS compliance_flag,
  (dry_solids_pct_blend < COALESCE(min_dry_matter_pct, dry_solids_pct_blend) + 0.3) AS alert_dry_band_flag,
  (defect_points_blend > COALESCE(max_defect_points, defect_points_blend) - 0.2) AS alert_defect_band_flag
FROM flags;
ALTER TABLE gold_blend_hourly_quality SET TBLPROPERTIES ('comment' = 'Hourly blend attributes vs PSS with compliance and alert band flags. Supports KPI, time series, and alert-driven narratives.');

-- b) gold_blend_suggestions: heuristic optimizer proxy for suggested fractions and adoption
CREATE OR REPLACE TABLE gold_blend_suggestions AS
WITH base AS (
  SELECT q.plant_id, q.line_id, q.sku_id, q.hour_bucket,
         q.sources, q.fractions,
         q.dry_solids_pct_blend, q.defect_points_blend, q.avg_length_mm_blend,
         q.potato_cost_per_kg_blend,
         q.sku_min_dry_solids_pct,
         q.compliance_flag
  FROM gold_blend_hourly_quality q
), sugg AS (
  SELECT *,
    CASE WHEN dry_solids_pct_blend > sku_min_dry_solids_pct + 0.4 THEN 0.05 ELSE CASE WHEN dry_solids_pct_blend < sku_min_dry_solids_pct THEN -0.07 ELSE 0.0 END END AS shift
  FROM base
), calc AS (
  SELECT plant_id, line_id, sku_id, hour_bucket,
         sources,
         transform(fractions, f -> greatest(0.0, least(1.0, f + shift))) AS suggested_fractions,
         potato_cost_per_kg_blend AS expected_cost_per_kg_usd,
         compliance_flag AS expected_compliance,
         fractions
  FROM sugg
)
SELECT
  plant_id,
  line_id,
  sku_id,
  hour_bucket,
  sources AS source_id_list,
  fractions AS actual_fractions,
  suggested_fractions,
  expected_cost_per_kg_usd,
  expected_compliance AS compliance_flag,
  ARRAY_MAX(transform(zip_with(fractions, suggested_fractions, (a, s) -> abs(a - s)), x -> x)) <= 0.03 AS adoption_flag,
  FALSE AS infeasible_flag,
  CAST(NULL AS STRING) AS infeasible_reason
FROM calc;
ALTER TABLE gold_blend_suggestions SET TBLPROPERTIES ('comment' = 'Per line-hour suggested blend fractions via heuristic optimizer and adoption flag (+/-3pp). Supports closed-loop action tracking and savings potential.');

-- c) gold_blend_hourly_kpis: daily aggregates and rates
CREATE OR REPLACE TABLE gold_blend_hourly_kpis AS
SELECT
  DATE_TRUNC('DAY', hour_bucket) AS date,
  plant_id,
  line_id,
  sku_id,
  COUNT(*) AS hours_total,
  COUNT_IF(q.compliance_flag) AS hours_in_spec,
  ROUND(100.0 * COUNT_IF(q.compliance_flag) / NULLIF(COUNT(*),0), 2) AS compliance_rate_pct,
  ROUND(100.0 * AVG(CAST(adoption_flag AS DOUBLE)), 2) AS adoption_rate_pct
FROM gold_blend_suggestions s
JOIN gold_blend_hourly_quality q USING (plant_id, line_id, sku_id, hour_bucket)
GROUP BY DATE_TRUNC('DAY', hour_bucket), plant_id, line_id, sku_id;
ALTER TABLE gold_blend_hourly_kpis SET TBLPROPERTIES ('comment' = 'Daily KPIs by plant/line/SKU: hourly compliance, adoption rate. Drives counter widgets and time-series joins.');

-- d) gold_blend_costs: raw potato cost per mt, daily
CREATE OR REPLACE TABLE gold_blend_costs AS
WITH hourly AS (
  SELECT plant_id, line_id, sku_id, hour_bucket,
         potato_cost_per_kg_blend,
         potato_cost_per_kg_blend * 1000.0 AS cost_per_mt_usd
  FROM gold_blend_hourly_quality
), daily AS (
  SELECT DATE_TRUNC('DAY', hour_bucket) AS date, plant_id, line_id, sku_id,
         AVG(cost_per_mt_usd) AS avg_cost_per_mt_usd
  FROM hourly
  GROUP BY DATE_TRUNC('DAY', hour_bucket), plant_id, line_id, sku_id
)
SELECT * FROM daily;
ALTER TABLE gold_blend_costs SET TBLPROPERTIES ('comment' = 'Daily average raw potato cost per mt by plant/line/SKU based on blended hourly cost proxy. Supports KPI and cost vs compliance chart.');

-- e) gold_downstream_penalties: weekly penalties from quality misses (synthetic shares per story)
CREATE OR REPLACE TABLE gold_downstream_penalties AS
WITH base AS (
  SELECT DATE_TRUNC('WEEK', hour_bucket) AS week_start, plant_id, line_id, sku_id,
         COUNT_IF(NOT compliance_flag) AS miss_hours
  FROM gold_blend_hourly_quality
  GROUP BY DATE_TRUNC('WEEK', hour_bucket), plant_id, line_id, sku_id
), alloc AS (
  SELECT week_start, plant_id, line_id, sku_id, miss_hours,
         miss_hours * 1500 AS total_penalties_usd
  FROM base
), explode_types AS (
  SELECT week_start, plant_id, line_id, sku_id, total_penalties_usd,
         stack(7,
           'repack', 0.38,
           'lower_price', 0.22,
           'reblend', 0.14,
           'blocked_stock', 0.08,
           're_sample', 0.08,
           'donation', 0.06,
           'dump', 0.12) AS (disposition_type, share)
  FROM alloc
)
SELECT week_start, plant_id, line_id, sku_id, disposition_type,
       ROUND(total_penalties_usd * share, 2) AS penalties_usd
FROM explode_types;
ALTER TABLE gold_downstream_penalties SET TBLPROPERTIES ('comment' = 'Weekly downstream disposition penalty costs split by type using narrative shares. Supports stacked bar and KPI.');

-- f) gold_supplier_variety_contributors: weekly supplier reliability KPIs
CREATE OR REPLACE TABLE gold_supplier_variety_contributors AS
WITH pss AS (
  SELECT LOWER(TRIM(plantid)) AS plant_id, productid AS sku_id,
         max_defect_points, min_dry_matter_pct
  FROM brlui.brian_lui_mccain.raw_product_specifications_pss
), joined AS (
  SELECT l.date, LOWER(TRIM(l.plant_id)) AS plant_id, LOWER(TRIM(l.intended_line_id)) AS line_id, l.intended_sku_id AS sku_id,
         l.supplier_id, LOWER(TRIM(l.variety)) AS variety, LOWER(TRIM(l.supplier_region)) AS supplier_region, l.gross_weight_kg,
         l.dry_solids_pct, l.defect_points,
         p.min_dry_matter_pct, p.max_defect_points
  FROM silver_potato_load_quality l
  LEFT JOIN pss p ON p.plant_id = LOWER(TRIM(l.plant_id)) AND p.sku_id = l.intended_sku_id
)
SELECT DATE_TRUNC('WEEK', date) AS week_start,
       plant_id, line_id, sku_id, supplier_id, variety, supplier_region,
       COUNT(*) AS loads,
       SUM(gross_weight_kg) AS volume_kg,
       ROUND(AVG(dry_solids_pct),2) AS mean_dry_solids_pct,
       ROUND(AVG(defect_points),2) AS mean_defect_points,
       ROUND(100.0 * COUNT_IF(dry_solids_pct < min_dry_matter_pct OR defect_points > max_defect_points) / NULLIF(COUNT(*),0), 2) AS mismatch_rate,
       COUNT_IF(dry_solids_pct < min_dry_matter_pct OR defect_points > max_defect_points) AS mismatch_loads
FROM joined
GROUP BY DATE_TRUNC('WEEK', date), plant_id, line_id, sku_id, supplier_id, variety, supplier_region;
ALTER TABLE gold_supplier_variety_contributors SET TBLPROPERTIES ('comment' = 'Weekly supplier/variety/region reliability KPIs: mismatch rate, averages, and volume. Supports supplier performance adjustment question.');

-- g) gold_global_filters_bridge: helper for filter UI
CREATE OR REPLACE TABLE gold_global_filters_bridge AS
SELECT DISTINCT 'gold_blend_hourly_quality' AS dataset_name, 'plant_id' AS field_name, plant_id AS field_value, CAST(hour_bucket AS DATE) AS date FROM gold_blend_hourly_quality
UNION ALL SELECT DISTINCT 'gold_blend_hourly_quality', 'line_id', line_id, CAST(hour_bucket AS DATE) FROM gold_blend_hourly_quality
UNION ALL SELECT DISTINCT 'gold_blend_hourly_quality', 'sku_id', sku_id, CAST(hour_bucket AS DATE) FROM gold_blend_hourly_quality
UNION ALL SELECT DISTINCT 'gold_blend_suggestions', 'source_id', explode(source_id_list) AS field_value, CAST(hour_bucket AS DATE) FROM gold_blend_suggestions
UNION ALL SELECT DISTINCT 'gold_supplier_variety_contributors', 'variety', variety, CAST(week_start AS DATE) AS date FROM gold_supplier_variety_contributors;
ALTER TABLE gold_global_filters_bridge SET TBLPROPERTIES ('comment' = 'Bridge for filters: plant_id, line_id, sku_id, source_id, variety across datasets and dates.');
