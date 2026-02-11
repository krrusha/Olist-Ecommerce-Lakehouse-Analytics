# Olist-Ecommerce-Lakehouse-Analytics

Databricks SQL lakehouse project analyzing **revenue concentration**, **fulfillment delays**, and **review impact** with an interactive **2-page dashboard**.



## Overview

This project uses the **Olist Brazilian E-Commerce Public Dataset** to build an end-to-end analytics workflow in **Databricks**:

- Load raw Olist tables into a source schema
- Create curated, analytics-ready tables/views for reporting
- Write SQL for KPI + operational insights
- Build a **2-page interactive Databricks Dashboard** with a global **Purchase Date Range** filter



## Tech Stack

- **Databricks SQL**
- **Delta tables / Views**
- **Databricks Dashboards** (parameter-driven filtering)
- **SQL modeling** (curated analytics layer)



## Dataset

**Olist Brazilian E-Commerce Public Dataset (Kaggle)**  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data



## Data Layers (simple & portfolio-friendly)

To keep the project clean and separated:

- **Source schema (raw loaded tables):** `workspace.olist_lakehouse`
- **Curated schema (analytics objects):** `workspace.olist_curated`

Key curated object:
- `workspace.olist_curated.analytics_orders_with_date`  
  Adds a clean `purchase_date` column (DATE) for dashboard filtering and analysis.



## Dashboard

### Global Filter
- **Purchase Date Range** (`p_date`)  
  A date-range parameter applied across all dashboard datasets using:
  `purchase_date BETWEEN :p_date.min AND :p_date.max`



## Page 1 — Overview: Revenue & Delivery Experience

**Goal:** High-level performance + what drives revenue + how delivery affects reviews.

Includes:
- KPI cards: Total Orders, Revenue (BRL), AOV (BRL), Avg Review Score, Avg Delivery Time, Late Delivery Rate
- Monthly Revenue Trend
- Top Categories by Revenue
- Avg Review Score by Delivery Delay Bucket



## Page 2 — Operations & Geography: Fulfillment Hotspots

**Goal:** Where demand is concentrated + where late-delivery risk is highest + which routes are hotspots.

Includes:
- Top Customer States by Revenue + Revenue Share tooltip
- Seller States with Highest Late Delivery Rate
- Worst Fulfillment Routes (Seller → Customer) + **Estimated Late Orders**
- Delivery Delay Distribution (% of items)
- Avg Review Score in Top Revenue States



## Key Insights (from the dashboard)

1. **Revenue is geographically concentrated.**  
   São Paulo (SP) contributes ~**37%** of revenue in the selected range.

2. **Late delivery is relatively low overall, but impactful.**  
   Roughly ~**9%** of items are delivered late; most late deliveries fall within **1–7 days**.

3. **Delivery delay strongly impacts customer satisfaction.**  
   Review scores drop significantly as delivery delays increase.

4. **Late-delivery risk is not evenly distributed across seller states.**  
   A small number of seller states show higher late delivery rates.

5. **Specific seller→customer routes are operational hotspots.**  
   Certain routes combine higher late-delivery rates with meaningful volume, increasing **Estimated Late Orders**.



## Recommendations

- Prioritize operational fixes on routes with high **Estimated Late Orders**
- Investigate seller-state bottlenecks (carrier performance, distance, logistics capacity)
- Protect top-revenue regions (like SP) with tighter delivery SLAs and fulfillment improvements
- Monitor delay buckets over time since they directly correlate with review score



## Dashboard Export

### PDF (recommended)
- **Page 1 — Overview:** [assets/olist_dashboard_overview.pdf](assets/olist_dashboard_overview.pdf)
- **Page 2 — Fulfillment Hotspots:** [assets/olist_dashboard_fulfillment_hotspots.pdf](assets/olist_dashboard_fulfillment_hotspots.pdf)

### Databricks Dashboard JSON (importable artifact)
- Export file(s) live in: `databricks_exports/`

> Note: The JSON export may include workspace-specific references (e.g., warehouse/dataset IDs). It’s included as an importable artifact for Databricks environments.



## Dashboard Screenshots

### Page 1 — Overview: Revenue & Delivery Experience
![Page 1](images/page1_overview.png)

### Page 2 — Operations & Geography: Fulfillment Hotspots
![Page 2 (Top)](images/page2_fulfillment_hotspots.png)  
![Page 2 (Bottom)](images/page2_fulfillment_hotspots_2.png)



## Reproducibility (SQL Scripts)

All SQL used to build the curated layer and dashboard datasets is included in `sql/`.

Recommended run order:

1. **Curated layer build (clean tables + analytics table + dashboard-ready view)**  
   - `sql/01_curated_views.sql`

2. **Dashboard datasets (Page 1)**  
   - `sql/02_dashboard_page1_datasets.sql`

3. **Dashboard datasets (Page 2)**  
   - `sql/03_dashboard_page2_datasets.sql`



## Repository Structure

```text
.
├── assets/               # PDF dashboard exports
├── databricks_exports/   # Databricks dashboard JSON export (importable)
├── images/               # Dashboard screenshots
├── sql/                  # SQL scripts to rebuild curated layer + dashboard datasets
└── README.md


