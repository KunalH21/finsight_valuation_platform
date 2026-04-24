## Plan: Profile Income Statement Data from S3 Bronze

Implement PySpark script to mount S3 Bronze, read income statement JSONs for 50 companies, print schema, and profile data quality: null fields and companies with fewer than 5 periods.

**Steps**
1. Configure PySpark session with S3 access using AWS credentials from Terraform outputs (finsight_ingestion_user access key/secret).
2. Read all JSON files from S3 Bronze bucket (finsight-bronze-418272773891) using wildcard path: s3a://bucket/financials/*/*/data.json.
3. Filter and select income_statement column from the DataFrame.
4. Print the schema of the income_statement struct.
5. Explode the income_statement map to rows: ticker, period_date, metric_name, value.
6. Profile nulls: Group by metric_name and count null values.
7. Profile periods: Group by ticker and count distinct period_dates; flag companies with count < 5.
8. Display results: schema, null counts per field, list of companies with insufficient periods.

**Relevant files**
- [spark/transform_financials.py](spark/transform_financials.py) — Implement the PySpark script here.
- [terraform/outputs.tf](terraform/outputs.tf) — Retrieve AWS credentials for S3 access.
- [ingestion/config.py](ingestion/config.py) — Reference bucket name and ticker list.

**Verification**
1. Run the script and verify S3 access works (no auth errors).
2. Confirm schema matches expected structure (struct with date string fields).
3. Check null counts are calculated correctly (e.g., some metrics may be null for certain periods).
4. Validate period counts: ensure companies with <5 periods are flagged (based on distinct date keys in income_statement).

**Decisions**
- Expected periods: 5 annual periods per company (as clarified).
- Data structure: income_statement is a map<string, struct> where keys are date strings (YYYY-MM-DD) and values are structs of metrics.
- Profiling scope: Focus on income_statement only, as requested.
- Credentials: Use Terraform outputs for access key/secret; assume they are available or run terraform output locally.