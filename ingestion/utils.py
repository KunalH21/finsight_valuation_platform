import boto3
import json
import logging
from ingestion import config

logger = logging.getLogger(__name__)


def stringify_keys(data):
    # If it's a dictionary → fix keys + go deeper
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            new_key = str(key)  # convert key to string
            new_dict[new_key] = stringify_keys(value)  # go deeper
        return new_dict

    # If it's a list → go through each item
    elif isinstance(data, list):
        return [stringify_keys(item) for item in data]

    # If it's anything else → leave it as is
    else:
        return data



def get_s3_client():
    return boto3.client('s3', region_name=config.AWS_REGION)


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
