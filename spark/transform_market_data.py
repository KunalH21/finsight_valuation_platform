import os
import logging
import boto3
import json

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("FinSight_Market_Transformation")


def get_spark():

    return SparkSession.builder \
        .appName("FinSight_Market_Transformations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()



def load_raw_json_from_s3(bucket, prefix, region="us-east-1"):

    s3 = boto3.client(
        "s3",
        region_name=region
    )

    paginator = s3.get_paginator("list_objects_v2")

    raw_data = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            try:
                response = s3.get_object(
                    Bucket=bucket,
                    Key=key
                )

                raw_data.append(
                    json.loads(
                        response["Body"].read()
                    )
                )

            except Exception as e:
                logger.warning(
                    f"Failed reading {key}: {e}"
                )


    return raw_data




def build_market_dataframe(spark, raw_data):

    records = []

    for record in raw_data:

        records.append({

            "ticker": record.get("ticker"),

            "price": record.get("price"),

            "market_cap": record.get("market_cap"),

            "price_date": record.get("price_date"),

            "ingestion_timestamp":
                record.get("ingestion_timestamp")
        })


    return spark.createDataFrame(
        [Row(**r) for r in records]
    )




def transform_market_data(df):

    return (

        df

        .withColumn(
            "price_date",
            to_date("price_date")
        )

        .withColumn(
            "price",
            col("price").cast(DoubleType())
        )

        .withColumn(
            "market_cap",
            col("market_cap").cast(DoubleType())
        )

        .withColumn(
            "year",
            year("price_date")
        )

        .withColumn(
            "month", date_format("price_date", "MM"))

    )





def sync_local_folder_to_s3(
    local_path,
    bucket,
    s3_prefix,
    region="us-east-1"
):

    s3_client = boto3.client(
        "s3",
        region_name=region
    )


    for root, _, files in os.walk(local_path):

        for file in files:

            full_path = os.path.join(
                root,
                file
            )

            relative_path = os.path.relpath(
                full_path,
                local_path
            )

            s3_key = os.path.join(
                s3_prefix,
                relative_path
            ).replace("\\","/")


            s3_client.upload_file(
                full_path,
                bucket,
                s3_key
            )


    logger.info(
        f"Synced output to s3://{bucket}/{s3_prefix}"
    )





def main():

    spark = get_spark()


    bronze_bucket = "finsight-bronze-layer"
    bronze_prefix = "market_data/"


    silver_bucket = "finsight-silver-layer"

    silver_local_path = "/tmp/finsight_market_silver"

    silver_prefix = "market_data/"


    raw_data = load_raw_json_from_s3(
        bronze_bucket,
        bronze_prefix
    )


    df_raw = build_market_dataframe(
        spark,
        raw_data
    )


    df_final = transform_market_data(
        df_raw
    )


    print(
        f"Refined {df_final.count()} market records into Silver."
    )


    df_final.write \
        .mode("overwrite") \
        .partitionBy(
            "year",
            "month"
        ) \
        .parquet(
            silver_local_path
        )


    sync_local_folder_to_s3(
        silver_local_path,
        silver_bucket,
        silver_prefix
    )


    spark.stop()



if __name__ == "__main__":
    main()