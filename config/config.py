"""
Configuration settings for the application.
"""
import os
import logging
from dataclasses import dataclass, field
from typing import Dict
from dotenv import load_dotenv, find_dotenv
from functools import lru_cache

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = find_dotenv()
if env_path:
    logger.info(f"Found .env file at: {env_path}")
    load_dotenv(env_path)
else:
    logger.warning("No .env file found!")

def get_default_table_pk_map() -> Dict[str, str]:
    """Get default table primary key mapping."""
    return {
        'admissions': 'hospitalization_case_number',
        'lab_results': 'result_id',
        'lab_tests': 'test_id',
        'patient_information': 'patient_id'
    }

@dataclass
class Config:
    # PostgreSQL settings
    postgres_host: str = os.getenv('POSTGRES_HOST', 'localhost')
    postgres_port: int = int(os.getenv('POSTGRES_PORT', '5432'))
    postgres_user: str = os.getenv('POSTGRES_USER', 'postgres')
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'postgres')
    postgres_db: str = os.getenv('POSTGRES_DB', 'postgres')
    
    # AWS settings
    aws_access_key_id: str = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key: str = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region: str = os.getenv('AWS_REGION', 'us-east-1')
    s3_bucket: str = os.getenv('S3_BUCKET', 'external-medate-exam-data')

    # Table primary key mapping
    table_pk_map: Dict[str, str] = field(default_factory=get_default_table_pk_map)

    def __post_init__(self):
        """Validate required configuration."""
        # Log AWS configuration status (safely)
        logger.info("Checking AWS credentials...")
        if self.aws_access_key_id:
            logger.info(f"AWS Access Key ID found (starts with: {self.aws_access_key_id[:4]}...)")
        else:
            logger.info("AWS_ACCESS_KEY_ID not found in environment variables or .env file")
            logger.info("Will attempt to use default AWS credential chain")

        if self.aws_secret_access_key:
            logger.info("AWS Secret Access Key found (value hidden)")
        else:
            logger.info("AWS_SECRET_ACCESS_KEY not found in environment variables or .env file")
            logger.info("Will attempt to use default AWS credential chain")
        
        logger.info(f"AWS Region set to: {self.aws_region}")
        logger.info(f"S3 Bucket set to: {self.s3_bucket}")

    @property
    def postgres_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

# Create a singleton instance
config = Config()