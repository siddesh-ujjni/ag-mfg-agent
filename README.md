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
🤖 This bundle was automatically created using the Databricks AI Demo Generator.

**Created**: 2025-11-04 22:26:52
**User**: brian.lui@databricks.com
