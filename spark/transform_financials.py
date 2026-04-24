import os
import json
import logging
import boto3

from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce

from fin_schema import income_statement_schema as s
import company_lookup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FinSight_Silver_Transformation")


def get_spark():
    return SparkSession.builder \
        .appName("FinSight_Silver_Transformations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def load_raw_json_from_s3(bucket, prefix, region="us-east-1"):
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")

    raw_data = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                raw_data.append(json.loads(response["Body"].read()))
            except Exception as e:
                logger.warning(f"Failed to read {key}: {e}")

    return raw_data


def build_raw_dataframe(spark, raw_data):
    filtered_data = []
    for record in raw_data:
        filtered_data.append({
            "ticker": record.get("ticker"),
            "ingestion_timestamp": record.get("ingestion_timestamp"),
            "income_statement": record.get("income_statement", {}),
            "balance_sheet": record.get("balance_sheet", {}),
            "cash_flow": record.get("cash_flow", {})
        })

    return spark.createDataFrame([Row(**r) for r in filtered_data], schema=s).cache()


def explode_statements(df_raw):
    df_is = df_raw.select("ticker", "ingestion_timestamp", explode("income_statement").alias("period_date", "metrics"))
    df_bs = df_raw.select("ticker", "ingestion_timestamp", explode("balance_sheet").alias("period_date", "metrics"))
    df_cf = df_raw.select("ticker", "ingestion_timestamp", explode("cash_flow").alias("period_date", "metrics"))

    return df_is.unionByName(df_bs).unionByName(df_cf).cache()


def normalize_metrics(df):
    return df.select(
        col("ticker"),
        col("ingestion_timestamp"),
        to_date(col("period_date")).alias("date"),
        quarter(to_date(col("period_date"))).alias("fiscal_quarter"),
        regexp_replace(lower(col("metric_name")), " ", "_").alias("metric_name"),
        col("value").cast(DoubleType()).alias("value")
    )


def attach_company_sectors(df, df_lookup):
    return df.join(broadcast(df_lookup), on="ticker", how="left")


def deduplicate_latest_records(df):
    window_spec = Window.partitionBy("ticker", "date", "metric_name").orderBy(desc("ingestion_timestamp"))
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")


def pivot_metrics_to_wide(df):
    return df.groupBy("ticker", "date", "fiscal_quarter", "sector") \
             .pivot("metric_name") \
             .agg(first("value"))


def calculate_derived_ratios(df):
    def safe_divide(numerator, denominator):
        return when(col(denominator).isNull() | (col(denominator) == 0), lit(None)) \
               .otherwise(col(numerator) / col(denominator))

    return df.withColumn(
        "ebitda",
        coalesce(
            col("ebitda"),
            col("operating_income") + col("depreciation_and_amortization")
        )
    ).withColumn(
        "gross_margin", safe_divide("gross_profit", "total_revenue")
    ).withColumn(
        "ebitda_margin", safe_divide("ebitda", "total_revenue")
    ).withColumn(
        "free_cash_flow", col("operating_cash_flow") - col("capital_expenditure")
    ).withColumn(
        "net_debt", col("total_debt") - col("cash_and_cash_equivalents")
    )


def flag_data_quality(df):
    core_metrics = [
        "ebitda",
        "operating_income",
        "gross_profit",
        "total_debt",
        "cash_and_cash_equivalents",
        "operating_cash_flow",
        "capital_expenditure"
    ]

    partial = lit(False)
    for m in core_metrics:
        if m in df.columns:
            partial = partial | col(m).isNull() | isnan(col(m))

    invalid = (
        col("total_revenue").isNull() |
        isnan(col("total_revenue")) |
        (col("total_revenue") <= 0)
    )

    return df.withColumn(
        "data_quality_flag",
        when(invalid, lit("INVALID"))
        .when(partial, lit("PARTIAL"))
        .otherwise(lit("CLEAN"))
    )


def sync_local_folder_to_s3(local_path, bucket, s3_prefix, region="us-east-1"):
    s3_client = boto3.client("s3", region_name=region)

    for root, _, files in os.walk(local_path):
        for file in files:
            full_local_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_local_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            s3_client.upload_file(full_local_path, bucket, s3_key)

    logger.info(f"Synced local output to s3://{bucket}/{s3_prefix}")


def main():
    spark = get_spark()

    bronze_bucket = "finsight-bronze-layer"
    bronze_prefix = "financials/"
    silver_local_path = "/tmp/finsight_silver"
    silver_bucket = "finsight-silver-layer"
    silver_prefix = "financial_statements/"

    raw_data = load_raw_json_from_s3(bronze_bucket, bronze_prefix)
    df_raw = build_raw_dataframe(spark, raw_data)
    df_exploded_periods = explode_statements(df_raw)

    df_exploded_metrics = df_exploded_periods.select(
        "ticker",
        "ingestion_timestamp",
        "period_date",
        explode("metrics").alias("metric_name", "value")
    ).cache()

    lookup_data = list(company_lookup.SECTOR_LOOKUP.items())
    df_lookup = spark.createDataFrame(lookup_data, schema=["ticker", "sector"])

    df_final = (
        df_exploded_metrics
        .transform(normalize_metrics)
        .transform(attach_company_sectors, df_lookup)
        .transform(deduplicate_latest_records)
        .transform(pivot_metrics_to_wide)
        .transform(calculate_derived_ratios)
        .transform(flag_data_quality)
        .withColumn("year", year(col("date")))
    )

    df_final.repartition(5, "sector") \
        .write \
        .mode("overwrite") \
        .partitionBy("sector", "year") \
        .parquet(silver_local_path)

    sync_local_folder_to_s3(silver_local_path, silver_bucket, silver_prefix)

    spark.stop()


if __name__ == "__main__":
    main()