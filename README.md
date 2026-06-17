# FinSight — Financial Data Engineering Platform

A production-inspired financial data platform that ingests, transforms, and models company financial data and market data into analytics-ready datasets.

FinSight demonstrates modern data engineering practices including:

- Workflow orchestration with Apache Airflow
- Cloud object storage based data lake architecture
- Distributed transformations using Apache Spark
- Analytics modeling with dbt
- Financial data quality validation
- Layered data architecture (Bronze → Silver → Analytics)

The project was built as a portfolio implementation to demonstrate how a Senior Data Engineer would design and structure a scalable financial analytics pipeline.

---

# Architecture Overview
A[Yahoo Finance API] --> B[Airflow DAG]

B --> C[Python Ingestion]

C --> D[S3 Bronze Layer]

D --> E[Apache Spark]

E --> F[S3 Silver Layer]

F --> G[Snowflake]

G --> H[dbt Transformations]

H --> I[Analytics Marts]

I --> J[Valuation Analytics]


Problem Statement

Financial datasets typically arrive in inconsistent formats and require:

reliable ingestion
schema normalization
historical tracking
validation
business-ready modeling

FinSight builds an end-to-end pipeline that converts raw financial data into analytics-ready valuation datasets.

Technology Stack
Orchestration

Apache Airflow

Used for:

scheduling ingestion workflows
dependency management
retries
pipeline monitoring

Implemented DAGs:

Quarterly financial ingestion pipeline
Daily market data ingestion pipeline
Storage

Amazon S3

Implemented as a data lake:

Bronze Layer:

raw API responses
immutable ingestion format

Silver Layer:

cleaned parquet datasets
partitioned analytical data
Processing

Apache Spark

Used for:

schema enforcement
normalization
deduplication
metric calculations
parquet generation
Transformation

dbt

Used for:

modular SQL transformations
dimensional modeling
analytics layer creation
reusable macros
Warehouse

Snowflake

Used as the analytical warehouse layer.

The project includes:

Snowflake loading tasks
dbt integration
analytical models

Data Pipeline
1. Ingestion Layer

Financial statements and market data are collected from Yahoo Finance.

The ingestion layer:

preserves raw responses
attaches ingestion metadata
handles ticker-level failures
uploads data into S3

Example Bronze structure: financials/
 └── year=2026/
      └── month=06/
           └── ticker=AAPL/
                └── data.json

2. Spark Silver Transformation

Spark converts raw JSON payloads into structured analytical datasets.

Processing includes:

flattening nested financial statements
normalizing metrics
adding sector metadata
removing duplicate records
calculating derived financial metrics

Generated metrics include:

EBITDA
Gross Margin
EBITDA Margin
Free Cash Flow
Net Debt

Output format:

Apache Parquet

Partitioning:

sector
year
3. dbt Analytics Layer

dbt follows a layered transformation approach:

RAW
 |
STAGING
 |
INTERMEDIATE
 |
MARTS
Staging Models

Purpose:

rename fields
cast data types
standardize schemas

Examples:

stg_income_statement
stg_balance_sheet
stg_cash_flow
stg_market_data
Intermediate Layer

Creates business logic models.

Example:

int_valuation_inputs

Combines:

financial statements
market data
calculated ratios

Calculations:

Enterprise Value
EV/EBITDA
P/E Ratio
Price/Book
Margin metrics
Analytics Marts

Consumer-facing datasets:

mart_valuation_multiples

Provides valuation comparison metrics.

mart_company_trend

Provides:

revenue growth
EBITDA growth
margin expansion
mart_sector_comps

Provides sector-level benchmarking.

Data Quality

Implemented checks:

Pipeline Level
retries in Airflow
failed ticker isolation
error logging
Data Level

Spark validates:

missing financial metrics
invalid revenue values
incomplete records

Quality states:

CLEAN
PARTIAL
INVALID

dbt tests validate:

primary fields
required dimensions
freshness expectations
Running Locally
Requirements
Docker
Docker Compose
AWS credentials
Snowflake credentials
Start Airflow
cd airflow

docker compose up

Airflow UI:

localhost:8080
Testing

Tests are written using pytest.

Coverage includes:

ingestion output validation
timestamp generation
S3 upload behavior
serialization handling

Run:

pytest
Engineering Highlights
Raw Data Preservation

Raw API responses are stored before transformation.

Benefits:

replay capability
debugging
schema evolution support
Partitioned Data Lake Design

Data is partitioned by:

time
ticker
business dimension

This reduces unnecessary scans as data volume grows.

Idempotent Pipeline Design

Tasks are designed to safely rerun:

deterministic transformations
overwrite-based outputs
controlled partitions
Current Limitations

This project is intentionally designed as a portfolio-scale implementation.

Current constraints:

local Docker deployment
controlled ticker universe
single-node Spark execution

A larger production deployment would introduce:

managed Spark cluster
object-store optimization
automated CI/CD
observability stack
schema registry
Future Improvements

Potential next steps:

migrate Spark jobs to Databricks / EMR
add Airflow task monitoring
introduce data contracts
add Great Expectations / Soda checks
add CI pipeline
add infrastructure as code
optimize incremental processing
