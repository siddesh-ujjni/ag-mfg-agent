# mccain - Databricks Asset Bundle

## Overview
coo of mccain foods requires closed-loop action suggestions in continuous production: model multi-source continuous deliveries with adjustable blend fractions, compare hourly blend quality at line entry against sku-specific minimum raw potato quality characteristics (pss), and suggest optimal source blending that just meets targets at minimal potato cost. emphasize cost-quality tradeoffs of raw potatoes and downstream additional costs when finished goods miss quality (actions include repack to different brand, sell at lower price, reblend into different product, blocked stock, re-sample, donation, dump). maintain 24h detection but prioritize proactive blend recommendations that prevent misses at lowest raw cost.

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

- **Genie Space** (ID: `01f0b5a5d15b1c71b05d5341b18c4ae5`)
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

- **Knowledge Assistant** (ID: `0d4d18d0-6f18-42db-98bd-562ab656a925`)
  - AI assistant with knowledge sources from Unity Catalog volumes
  - Vector search-powered retrieval augmented generation (RAG)
  - Example questions and guidelines included

- **Multi-Agent Supervisor** (ID: `bc0deddc-a9d0-4180-bfe7-dff3e1a89291`)
  - Orchestrates multiple specialized agents
  - Routes queries to appropriate sub-agents (Genie, KA, endpoints)
  - Complex multi-step workflows supported

### Dashboards
This bundle includes Lakeview dashboards:
- **closed-loop blend optimization - mccain idaho** - Business intelligence dashboard with visualizations

### PDF Documents
No PDF documents are included in this demo.

## Configuration

### Unity Catalog
- **Catalog**: `brlui`
- **Schema**: `brian_lui_mccain`
- **Workspace Path**: `/Users/brian.lui@databricks.com/mccain`

### Customization
You can modify the bundle by editing `databricks.yml`:
- Change target catalog/schema in the `variables` section
- Adjust cluster specifications for data generation
- Add additional tasks or resources

## Key Questions This Demo Answers
1. hourly monitoring: which plant/line/sku hours show blend quality approaching or breaching tolerance bands vs pss (dry solids, defect points, color), and when did alerts trigger?
2. automated alerts & escalation: what thresholds (e.g., dry solids within 0.3 pp of min, defect points within 0.2 of max) generated alerts, what corrective actions were suggested (fraction shifts, reroute, slow-down), and how quickly were they executed?
3. cost impact simulation: for hours exceeding cost thresholds, what is the projected raw cost vs optimal blend cost, what is the delta per mt, and which fraction adjustments reduce cost while staying within specs?
4. supplier performance adjustment: which suppliers/varieties exhibit consistent quality variance and cost impact over the last 8 weeks, and how should blending priority or procurement allocations be adjusted?
5. suggested actions vs adoption: how often were suggested fraction changes applied within tolerance (+/-3 pp), and what compliance and cost improvements followed?
6. alert policy effectiveness: did escalation levels (operator -> supervisor -> quality manager) reduce time-to-correct when tolerance bands were breached, and by how much per line?
7. risk outlook: given current availability, which upcoming hours are likely to hit alert thresholds, and what pre-emptive actions (blend shift, reroute, supplier swap) are recommended?

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

**Created**: 2025-10-28 17:11:24
**User**: brian.lui@databricks.com
