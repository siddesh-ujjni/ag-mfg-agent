# mccain_ebc - Databricks Asset Bundle

## Overview
McCain Foods demo for a multi-agent manufacturing optimization system: detect under/over-quality production lines in near-real-time, quantify raw potato cost implications, and propose human-review scenario plans (load reallocation and equipment upgrades). Between 2025-07-10 and 2025-10-31, the system flags several lines where blend quality exceeded targets (over-quality) and others where it fell short (under-quality), aligning anomalies to raw load quality variability, OSI PI line quality signals, OEE downtime patterns, and equipment throughput constraints. Impact quantified: ~$1.8M avoidable raw material cost from persistent over-quality and ~$0.9M risk from under-quality rework/downgrades. The dashboard ties ingredient quality -> line signals -> equipment/OEE -> dated events -> financial exposure.

## Deployment

This bundle can be deployed to any Databricks workspace using Databricks Asset Bundles (DAB):

### Prerequisites
1. **Databricks CLI**: Install the latest version
   ```bash
   pip install databricks-cli
   ```
2. **Authentication**: Configure your workspace credentials
   ```bash
   databricks configure
   ```
3. **Workspace Access**: Ensure you have permissions for:
   - Unity Catalog catalog/schema creation
   - SQL Warehouse access
   - Workspace file storage

### Deploy the Bundle
```bash
# Navigate to the dab directory
cd dab/

# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace (--force-lock to override any existing locks)
databricks bundle deploy --force-lock

# Run the data generation workflow
databricks bundle run demo_workflow
```

The deployment will:
1. Create Unity Catalog resources (schema and volume)
2. Upload PDF files to workspace (if applicable)
3. Deploy job and dashboard resources

The workflow will:
1. Create Unity Catalog catalog if it doesn't exist (DAB doesn't support catalog creation)
2. Generate synthetic data using Faker and write to Unity Catalog Volume
3. Execute SQL transformations (bronze → silver → gold)
4. Deploy agent bricks (Genie spaces, Knowledge Assistants, Multi-Agent Supervisors) if configured

## Bundle Contents

### Core Files
- `databricks.yml` - Asset bundle configuration defining jobs, dashboards, and deployment settings
- `bricks_conf.json` - Agent brick configurations (Genie/KA/MAS) if applicable
- `agent_bricks_service.py` - Service for managing agent brick resources (includes type definitions)
- `deploy_resources.py` - Script to recreate agent bricks in the target workspace

### Data Generation
- Python scripts using Faker library for realistic synthetic data
- Configurable row counts, schemas, and business logic
- Automatic Delta table creation in Unity Catalog

### SQL Transformations
- `transformations.sql` - SQL transformations for data processing
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) medallion architecture
- Views and tables for business analytics

### Agent Bricks
This bundle includes AI agent resources:

- **Genie Space** (ID: `01f0b9cf5c291e4cbc5e2de1c372cca1`)
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

- **Knowledge Assistant** (ID: `338a2c1b-1787-4632-8839-a75f48df11aa`)
  - AI assistant with knowledge sources from Unity Catalog volumes
  - Vector search-powered retrieval augmented generation (RAG)
  - Example questions and guidelines included

- **Multi-Agent Supervisor** (ID: `244c3742-55f0-4ee7-b500-bab88d4e2b25`)
  - Orchestrates multiple specialized agents
  - Routes queries to appropriate sub-agents (Genie, KA, endpoints)
  - Complex multi-step workflows supported

### Dashboards
This bundle includes Lakeview dashboards:
- **Production Quality, OEE, and Cost Exposure** - Business intelligence dashboard with visualizations

### PDF Documents
No PDF documents are included in this demo.

## Configuration

### Unity Catalog
- **Catalog**: `demo_generator`
- **Schema**: `brian_lui_mccain_ebc`
- **Workspace Path**: `/Users/brian.lui@databricks.com/mccain_ebc`

### Customization
You can modify the bundle by editing `databricks.yml`:
- Change target catalog/schema in the `variables` section
- Adjust cluster specifications for data generation
- Add additional tasks or resources

## Key Questions This Demo Answers
1. When did each line deviate from PSS targets, and by how many percentage points relative to tolerance?
2. Which plants/lines show persistent over-quality vs under-quality, and what supplier/load mix changes align to those deviations?
3. What are the incremental raw potato cost implications of over-quality and the rework/downgrade exposure from under-quality during 2025-08-18..2025-09-12?
4. How do OEE components (availability, performance, quality) move around the deviation windows, and which downtime codes correlate with quality drift?
5. What scenario actions (load reallocation or equipment upgrade) yield the largest savings while maintaining finished product quality?
6. How much high-DM load can be reallocated intra-region without violating logistics constraints, and what is the expected blend DM by hour after reallocation?
7. Which equipment upgrades on NA-ID-P01 L3 offer the best ROI in stabilizing throughput and reducing defects?

## Deployment to New Workspaces

This bundle is **portable** and can be deployed to any Databricks workspace:

1. The bundle will recreate all resources in the target workspace
2. Agent bricks (Genie/KA/MAS) are recreated from saved configurations in `bricks_conf.json`
3. SQL transformations and data generation scripts are environment-agnostic
4. Dashboards are deployed as Lakeview dashboard definitions

Simply run `databricks bundle deploy` in any workspace where you have the required permissions.

## Troubleshooting

### Common Issues

**Bundle validation fails:**
- Ensure `databricks.yml` has valid YAML syntax
- Check that catalog and schema names are valid
- Verify warehouse lookup matches an existing warehouse

**Agent brick deployment fails:**
- Check that `bricks_conf.json` exists and contains valid configurations
- Ensure you have permissions to create Genie spaces, KA tiles, and MAS tiles
- Verify vector search endpoint exists for Knowledge Assistants

**SQL transformations fail:**
- Ensure the catalog and schema exist in the target workspace
- Check warehouse permissions and availability
- Review SQL syntax for Unity Catalog compatibility (3-level namespace: `catalog.schema.table`)

### Getting Help
- Review Databricks Asset Bundles documentation: https://docs.databricks.com/dev-tools/bundles/
- Check the generated code in this bundle for implementation details
- Contact your Databricks workspace administrator for permissions issues

## Generated with AI Demo Generator

Prompt:

A demo for McCain Foods that demonstrates a multi-agent system that detects under- or over-quality potato product production lines, assesses the cost implications, and provides detailed suggestions for human driven scenario analysis. Examples of scenario analysis include raw potato load reallocation at a plant/line level for optimizing potato supply and upgrading manufacturing equipment for select plants/lines in order to improve efficiency and product quality.

The system has access to the following information:
1. Product quality targets and thresholds from Product Specific Specifications (PSS) data 
2. Raw potato quality (actual delivered load quality)
3. Plant production line quality (from OSI PI). 
4. Line equipment data that contains different potato manufacturing equipments per line and their throughput
5. Overall equipment efficiency (OEE) data (with information on no downtime production runs and downtime events)
6. Equipment manuals with performance (efficiency) specifications for existing and similar production line equipment 

--Additional Background--
McCain Foods runs continuous production of finished goods, where one of the ingredients (raw potatoes) carries the bulk of the cost. The cost of the potatoes depends mostly on its quality: the higher the quality, the larger the cost. On the other hand, the ingredient quality is a major factor impacting the quality of the finished product. So for each finished product SKU, certain minimal values for a number of quality characteristics of the raw potato are set. And if the finished product does not meet the quality criteria, then also additional cost may result. For example, the product may need to be repacked to a different brand, sold at a lower price, put back into the line and blended into a different product, put temporarily on blocked stock, re-sampled, put to donation or even dumped, just to name a few. So the ideal scenario is delivering the key ingredient just meeting its quality targets. This would potentially result in the adequate finished product quality without unnecessarily impacting the cost of the raw material. The raw material entering the line is delivered continuously to the production facility from different sources, and is blended to achieve the above goal. The fraction of each source in the blend can be adjusted. We can monitor the quality of the blend entering the production line in hourly intervals and put the result against the raw material quality specs.

@datasources
You have access to the following tables only, with this exact schema:

CREATE TABLE raw_line_quality_events_osipi ( 
	datetime TIMESTAMP, 
	plant_id INT,
	plant_name STRING, 
	product_id STRING, 
	product_name STRING,
	variety STRING, 
	avg_length_mm INT, 
	usda_color_0 BIGINT, 
	usda_color_1 BIGINT, 
	usda_color_2 BIGINT, 
	usda_color_3 BIGINT, 
	usda_color_4 BIGINT, 
	total_defect_points BIGINT, 
	dry_solids_pct DOUBLE COMMENT "Percentage of potato dry matter", 
) USING DELTA 
COMMENT 'Raw table for raw_line_quality_events_osipi data'

CREATE TABLE raw_potato_load_quality ( 
	plant_id INT, 
	plant_name STRING, 
	VarietyLabel STRING COMMENT "for example: Zorba 35mm"
	load_number STRING COMMENT "load id", 
	effective_actual_weight INT COMMENT "Net weight in tons",
	average_length_grading_mm INT "Average length grading in mm", 
	usda_color_0_pct DOUBLE COMMENT "Percentage of potatoes of USDA color 0", 
	usda_color_1_pct DOUBLE COMMENT "Percentage of potatoes of USDA color 1", 
	usda_color_2_pct DOUBLE COMMENT "Percentage of potatoes of USDA color 2", 
	usda_color_3_pct DOUBLE COMMENT "Percentage of potatoes of USDA color 3", 
	usda_color_4_pct DOUBLE COMMENT "Percentage of potatoes of USDA color 4", 
	total_defect_points INT COMMENT "Total number of defect points on potatoes", 
	dry_matter_pct DOUBLE COMMENT "Percentage of potato dry matter"
) USING delta 
COMMENT 'Raw table for raw_potato_load_quality data'

CREATE TABLE raw_product_specifications_pss ( 
	plant_id INT,
	plant_name STRING, 
	plant_line STRING,
	product_id BIGINT COMMENT "Product SKU",
	product_name STRING
	average_length_grading_mm_min INT COMMENT "Minimum average length grading in mm", 
	average_length_grading_mm_target INT COMMENT "Target average length grading in mm", 
	average_length_grading_mm_max INT COMMENT "Maximum average length grading in mm", 
	pct_min_50mm_length DOUBLE COMMENT "Percentage of product comprised of potatoes longer than 50mm and less than 75mm", 
	pct_min_75mm_length DOUBLE COMMENT "Percentage of product comprised of potatoes longer than 75mm", 
	max_usda_color_0 INT COMMENT "Maximum quantity of potatoes of USDA color 0 per unit of product", 
	max_usda_color_1 INT COMMENT "Maximum quantity of potatoes of USDA color 1 per unit of product", 
	max_usda_color_2 INT COMMENT "Maximum quantity of potatoes of USDA color 2 per unit of product", 
	max_usda_color_3 INT COMMENT "Maximum quantity of potatoes of USDA color 3 per unit of product", 
	max_usda_color_4 INT COMMENT "Maximum quantity of potatoes of USDA color 4 per unit of product", 
	max_defect_points INT COMMENT "Maximum number of defect points on potatoes", 
	min_dry_matter_pct DOUBLE COMMENT "Minimum allowable percentage of potato dry matter", 
	max_dry_matter_pct DOUBLE COMMENT "Maximum allowable percentage of potato dry matter", 
	approved_potato_varieties ARRAY<STRING> COMMENT "Allowable potato varieties that can be blended together for this product"
) USING delta 
COMMENT 'Raw table for raw_product_specifications_pss data'

CREATE TABLE raw_oee_production_runs_and_downtime_events ( 
	datetime TIMESTAMP, 
	plant_id INT, 
	plant_name STRING, 
	plant_line STRING, 
	downtime_cat_1 STRING COMMENT "The first-level downtime category. (e.g. no downtime, Mechanical, Operational, Idle Time, changover)", 
	downtime_cat_2 STRING COMMENT "The second-level downtime category. e.g. no downtime, equipment calibration, idle time, scheduled sanitation and maintenance", 
	product_name STRING, 
	start_time TIMESTAMP COMMENT "The timestamp marking the beginning of the production run or downtime event.", 
	end_time STRING "The timestamp marking the end of the production run or downtime event.", 
	duration BIGINT "The total elapsed time of the production run or event.", 
	downtime_duration BIGINT "The portion of duration in minutes in which the line was in a downtime state.", 
	qty_packed DOUBLE "The weight in tons of quantity packed during the run.", 
	num_totes_on DOUBLE "The count of totes placed onto the line during the run."
) USING delta 
COMMENT 'Raw table for raw_oee_equipment_events data'

CREATE TABLE raw_line_equipment ( 
	plant_id INT, 
	plant_name STRING, 
	plant_line STRING, 
	equipment_id STRING,
	equipment_type STRING,
	equipment_description STRING,
	equipment_rated_throughput_tph DOUBLE,
	commission_date DATE
) USING delta 
COMMENT 'Raw table for raw_line_equipment data'

🤖 This bundle was automatically created using the Databricks AI Demo Generator.

**Created**: 2025-11-04 22:26:52
**User**: brian.lui@databricks.com
