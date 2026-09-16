import boto3
import json
import datetime
import pandas as pd
import logging
from ingestion import config
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


def stringify_all(data):
    """
    Recursively converts all Timestamps (keys & values) to strings.
    This ensures the data is 100% compatible with JSON.
    """
    if isinstance(data, dict):
        return {str(k): stringify_all(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [stringify_all(i) for i in data]
    elif isinstance(data, (pd.Timestamp, datetime.datetime)):
        # If we find a 'living clock', turn it into a string!
        return data.isoformat()
    else:
        # If it's a normal number or string, leave it alone.
        return data



def upload_to_s3(data: dict, ticker: str, data_type: str = "financials"):
    """Standardized tool using S3Hook with deep serialization."""
    try:
        hook = S3Hook(aws_conn_id='aws_default')
        
        # We clean EVERYTHING before dumping to JSON
        clean_data = stringify_all(data)
        json_payload = json.dumps(clean_data)
        now = datetime.datetime.now()
        file_key = f"{data_type}/year={now.year}/month={now.month:02d}/ticker={ticker}/data.json"
        
        hook.load_string(
            string_data=json_payload,
            key=file_key,
            bucket_name="finsight-bronze-layer",
            replace=True
        )
        logging.info(f"Successfully uploaded {ticker} {data_type} to S3.")
    except Exception as e:
        logging.error(f"Failed upload for {ticker}: {e}")
        raise e
