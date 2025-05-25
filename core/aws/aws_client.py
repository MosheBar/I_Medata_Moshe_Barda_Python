"""
AWS client for S3 operations.
"""
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from io import BytesIO
from typing import Dict, Any, List, Optional
from config.config import config
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)

class AWSClient:
    def __init__(self):
        try:
            logger.info("Initializing AWS S3 client...")
            
            # Only pass credentials if they are explicitly set in config
            if config.aws_access_key_id and config.aws_secret_access_key:
                self.s3 = boto3.client(
                    's3',
                    aws_access_key_id=config.aws_access_key_id,
                    aws_secret_access_key=config.aws_secret_access_key,
                    region_name=config.aws_region
                )
            else:
                # Use default credential chain (environment, ~/.aws/credentials, etc.)
                self.s3 = boto3.client('s3', region_name=config.aws_region)
            
            # Test the credentials by checking access to the specific bucket we need
            logger.info(f"Testing AWS credentials by checking access to bucket: {config.s3_bucket}")
            try:
                self.s3.head_bucket(Bucket=config.s3_bucket)
                logger.info("Successfully verified bucket access")
            except ClientError as e:
                error = e.response['Error']
                if error['Code'] == '403':
                    logger.warning("Limited permissions detected - proceeding with restricted access")
                elif error['Code'] == '404':
                    raise ValueError(f"Bucket {config.s3_bucket} does not exist")
                else:
                    raise
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS Error: {error_code} - {error_message}")
            
            if error_code == 'InvalidAccessKeyId':
                raise ValueError("Invalid AWS credentials. Please check your AWS credentials in environment variables, .env file, or ~/.aws/credentials")
            elif error_code == 'SignatureDoesNotMatch':
                raise ValueError("AWS credentials are not valid. Please check your AWS secret key")
            elif error_code == '403':
                logger.warning("Limited permissions detected - proceeding with restricted access")
            else:
                raise

    def read_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a Parquet file from S3 into a pandas DataFrame."""
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            buffer = BytesIO(response['Body'].read())
            table = pq.read_table(buffer)
            return table.to_pandas()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"File not found in S3: {bucket}/{key}")
            elif e.response['Error']['Code'] == '403':
                raise PermissionError(f"Access denied to S3 object: {bucket}/{key}")
            raise

    def write_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> None:
        """Write a pandas DataFrame to S3 as a Parquet file."""
        try:
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            self.s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise ValueError(f"S3 bucket does not exist: {bucket}")
            elif e.response['Error']['Code'] == '403':
                raise PermissionError(f"Access denied to write to S3: {bucket}/{key}")
            raise

    def list_objects(self, bucket: str, prefix: str = '') -> List[str]:
        """List objects in an S3 bucket with given prefix."""
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents']]
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                logger.warning(f"Access denied to list objects in {bucket}/{prefix}")
                return []
            raise

    def delete_s3_object(self, bucket: str, key: str) -> None:
        """Delete an object from S3."""
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise PermissionError(f"Access denied to delete S3 object: {bucket}/{key}")
            raise

    def get_parquet_schema(self, bucket: str, key: str) -> Dict[str, str]:
        """Get schema information from a Parquet file."""
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            buffer = BytesIO(response['Body'].read())
            parquet_schema = pq.read_schema(buffer)
            
            schema_dict = {}
            for field in parquet_schema:
                schema_dict[field.name] = str(field.type)
            
            return schema_dict
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise PermissionError(f"Access denied to read S3 object schema: {bucket}/{key}")
            raise

    def copy_object(self, source_bucket: str, source_key: str,
                   dest_bucket: str, dest_key: str) -> None:
        """Copy an object within S3."""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise PermissionError(f"Access denied to copy S3 object from {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")
            raise 