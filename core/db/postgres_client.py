"""
PostgreSQL client for database operations.
"""
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd
from typing import List, Dict, Any
from config.config import config
from contextlib import contextmanager
import psycopg2
from urllib.parse import urlparse

class PostgresClient:
    """Client for PostgreSQL database operations."""

    def __init__(self, connection_string: str):
        """Initialize PostgreSQL client."""
        # Parse connection string
        result = urlparse(connection_string)
        self.db_params = {
            'dbname': result.path[1:],
            'user': result.username,
            'password': result.password,
            'host': result.hostname,
            'port': result.port or 5432
        }
        
        # Test connection with psycopg2 first
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.close()
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}")
            
        # Create SQLAlchemy engine if psycopg2 connection succeeds
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5
        )
        self.schema = 'medate_exam'  # Set default schema

    def get_table_schema(self, table_name: str, schema: str = 'medate_exam') -> List[Dict[str, Any]]:
        """Get schema information for a table."""
        query = text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema
            AND table_name = :table
            ORDER BY ordinal_position
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'schema': schema, 'table': table_name})
            return [dict(row) for row in result]

    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', schema: str = None) -> None:
        """Write a DataFrame to a table."""
        schema = schema or self.schema
        with self.engine.begin() as conn:
            df.to_sql(table_name, conn, schema=schema, if_exists=if_exists, index=False)

    @contextmanager
    def transaction(self):
        """Create a transaction context that automatically commits or rolls back."""
        conn = self.engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a table into a DataFrame."""
        query = f"SELECT * FROM {self.schema}.{table_name}"
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            if result.returns_rows:
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result]
            return []

    def table_exists(self, table_name: str, schema: str = 'medical') -> bool:
        """Check if a table exists."""
        query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'schema': schema, 'table': table_name})
            return result.scalar()

    def __del__(self):
        """Ensure engine is properly disposed when client is deleted."""
        if hasattr(self, 'engine'):
            self.engine.dispose() 