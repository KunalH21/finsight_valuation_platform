
## Overview

This document explains the engineering decisions behind FinSight.

The goal was to design a realistic financial data platform using patterns commonly found in production data engineering environments.

---

# 1. Why a Layered Data Architecture?

Decision:

Use:

Bronze → Silver → Analytics


Reason:

Raw financial APIs are unstable and change over time.

Keeping layers separated provides:

- replay capability
- debugging ability
- controlled transformations


---

# 2. Why Store Raw Data First?

Decision:

Persist API responses before transformation.


Reason:

Financial data sources may change schemas.

By preserving raw payloads:

- transformations can be rebuilt
- historical ingestion is reproducible
- debugging becomes easier


Trade-off:

Storage cost increases.

For enterprise systems this is usually acceptable because object storage is cheap.

---

# 3. Why Apache Spark?

Decision:

Use Spark for transformation.


Reason:

Financial datasets involve:

- multiple statements
- historical records
- joins
- aggregations


Spark provides:

- distributed processing
- scalable execution model
- dataframe transformations


Trade-off:

For small datasets Spark adds operational overhead.

The choice was made because the architecture should scale beyond the initial dataset.

---

# 4. Why dbt?

Decision:

Use dbt for analytics modeling.


Reason:

dbt provides:

- modular SQL
- dependency management
- testing
- documentation


Separating Spark transformations from business modeling keeps responsibilities clear:

Spark:
data processing

dbt:
analytics logic


---

# 5. Why S3 Data Lake Storage?

Decision:

Use object storage as the central storage layer.


Reason:

Object storage provides:

- low cost
- scalability
- separation of compute and storage


The design supports future migration to:

- AWS Glue
- Athena
- EMR
- Databricks


---

# 6. Partitioning Strategy

Bronze: data_type/year/month/ticker
Purpose: Reduce unnecessary scans.

Silver: sector/year
Purpose:Optimize analytical workloads.


Trade-off:

Too many partitions can create small files.

Future improvement:

Introduce optimized file sizing and compaction.

---

# 7. Failure Handling Strategy

Ingestion failures are isolated per ticker.

Example:

If one company fails:

- log error
- continue processing remaining tickers


Reason:

External APIs are unreliable.

One bad record should not stop the entire pipeline.


---

# 8. Data Quality Approach

Quality checks happen at multiple stages.


Ingestion:

Validate response availability.


Spark:

Validate financial completeness.


dbt:

Validate analytical outputs.


This follows the principle:

"Detect quality issues as early as possible."

---

# 9. Idempotency

Pipeline jobs are designed to be rerunnable.


Approach:

- deterministic partition paths
- overwrite-based transformations
- latest-record deduplication


Benefit:

Failures can be recovered without manual cleanup.

---

# 10. Scalability Considerations

Current:

Local Docker execution.

Designed patterns:

- cloud object storage
- distributed processing
- warehouse modeling


Scaling path:

Small:

Docker + local Spark


Medium:

Managed Airflow + cloud Spark


Large:

Databricks/EMR + optimized lakehouse architecture


---

# 11. Trade-offs

## Simplicity vs Scale

The implementation avoids unnecessary infrastructure complexity.

For a portfolio project:

learning production patterns is more valuable than adding operational overhead.


## Raw JSON vs Strict Schemas

Raw ingestion preserves flexibility.

Structured schemas are applied later.


Trade-off:

More downstream transformation work.

---

# 12. What I Would Change in Production

At larger scale I would add:

- infrastructure as code
- CI/CD
- data observability
- automated lineage
- schema enforcement
- incremental processing
- monitoring dashboards

---

# Summary

FinSight demonstrates the design principles expected from modern data platforms:

- reliable ingestion
- scalable storage patterns
- distributed processing
- modular transformations
- quality controls
- maintainable architecture
