# Mining-Analytics-Pipeline
Sustainable Mining Analytics Pipeline – ETL for Global Production Efficiency and Environmental Risk Assessment.

This project demonstrates modern data engineering skills by ingesting real-world mining data, calculating industry-relevant KPIs (waste-to-production ratio, efficiency scores, risk flags), serving the data through a FastAPI REST API, and delivering an interactive Excel dashboard for operations and sustainability teams.

### The design prioritizes:

Simplicity (runs locally with zero cloud cost)
Reproducibility (one main script + requirements.txt)
Demonstrable data engineering skills (multi-source ingestion, cleaning/joins/calculations, relational modeling, SQL analytics, Excel BI)
Mining-industry relevance (production efficiency + environmental risk proxies from real mine data)

## Tech Stack:

ETL & Processing: Python + pandas\ 
Database: SQLite (lightweight & file-based)\
API Layer: FastAPI + Uvicorn + Pydantic\
Analytics: SQL queries exposed via API\
Visualization: Microsoft Excel (live API pull via Power Query)

## High-Level Data Flow

### Extract
Multiple raw CSV files (facilities, production, reserves, waste, etc.) from the Open Database on Global Coal and Metal Mining are read into pandas DataFrames.

### Transform
Merge datasets on common keys (e.g., mine_id)
Clean & standardize (handle missing values, date parsing, unit consistency)
Engineer business KPIs relevant to mining:
Waste-to-production ratio (waste_ratio)
Year-over-year production change
Risk flags (high waste ratio, declining output)

Filter recent years (2015+) and aggregate where needed

### Load
Cleaned & enriched data is written into a local SQLite database (production.db).
API Layer: FastAPI + Uvicorn (server) + Pydantic (for request/response models and validation)
Tables follow a simple star-like schema:
Dimension: dim_facilities (mine metadata: country, owner, material)
Fact: fact_production (time-series metrics + calculated KPIs)

## Analyze & Visualize
Analytical SQL queries (stored in queries.sql) answer mining-relevant questions
Final output exported to Excel → pivot tables, slicers, charts, conditional formatting for interactive dashboard
