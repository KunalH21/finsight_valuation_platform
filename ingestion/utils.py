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
    Recursively converts ALL Timestamps (keys AND values) to strings.
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


'''
def get_s3_client():
    return boto3.client('s3', region_name=config.AWS_REGION)
'''

'''
def upload_to_s3(data: dict, ticker: str, s3_client=None):
    """Standardized tool to upload financial data to S3 Bronze."""
    s3 = s3_client or get_s3_client()

    # 1. Derive the year from the metadata for reprocessability
    # data['ingestion_timestamp'] looks like '2024-10-27...'
    year = data['ingestion_timestamp'][:4]
    
    # 2. Construct the Hive-style partition path [6]
    # Format: financials/year=YYYY/ticker=TICKER/data.json
    key = f"financials/year={year}/ticker={ticker}/data.json"
    
    try:
        clean_data = stringify_keys(data)
        # Convert the dictionary to a JSON string
        json_data = json.dumps(clean_data, default=str)  # default=str to handle any non-serializable data types
        
        # Upload to the bucket defined in config.py
        s3.put_object(
            Bucket=config.S3_BRONZE_BUCKET,
            Key=key,
            Body=json_data
        )
        logger.info(f"Successfully landed {ticker} in S3: {key}")
        
    except Exception as e:
        logger.error(f"Failed to upload {ticker} to S3: {e}")
        # In production, we might raise the error here to stop the job
        raise
'''

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

        
'''
def upload_to_s3(data: dict, ticker: str, data_type: str = "financials"):
    """
    Standardized tool using S3Hook. 
    Accepts 'data_type' to partition by 'financials' or 'market_data' [4].
    """
    try:
        hook = S3Hook(aws_conn_id='aws_default')
        clean_data = stringify_all(data)
        now = datetime.datetime.now()
        
        # This builds: market_data/year=2026/month=06/ticker=AAPL/data.json [5]
        file_key = f"{data_type}/year={now.year}/month={now.month:02d}/ticker={ticker}/data.json"
        
        hook.load_string(
            string_data=json.dumps(clean_data),
            key=file_key,
            bucket_name="finsight-bronze-layer",
            replace=True
        )
    except Exception as e:
        logging.error(f"S3 Upload failed for {ticker}: {e}")
        raise e
        '''