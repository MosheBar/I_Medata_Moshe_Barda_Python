"""
Base validator class for test assertions.
"""
from datetime import datetime
from typing import Any, List, Union, Type, Optional
import pandas as pd

class ValidationError(Exception):
    """Custom validation error."""
    pass

class BaseValidator:
    """Base validator class for test assertions."""
    
    def validate_value_equality(self, actual: Any, expected: Any, field_name: str) -> None:
        """Validate that two values are equal."""
        if actual != expected:
            raise ValidationError(
                f"Value mismatch for {field_name}: Expected '{expected}', got '{actual}'"
            )
    
    def validate_required_fields(self, data: dict, required_fields: List[str]) -> None:
        """Validate that all required fields are present in the data."""
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise ValidationError(
                f"Missing required fields: {', '.join(missing_fields)}"
            )
    
    def validate_pattern(self, actual: str, expected_pattern: str, field_name: str) -> None:
        """Validate that a string matches an expected pattern."""
        if actual != expected_pattern:
            raise ValidationError(
                f"Pattern mismatch for {field_name}: Expected '{expected_pattern}', got '{actual}'"
            )
    
    def validate_range(
        self,
        value: Union[int, float, datetime],
        min_value: Union[int, float, datetime],
        max_value: Union[int, float, datetime],
        field_name: str
    ) -> None:
        """Validate that a value is within an expected range."""
        if not min_value <= value <= max_value:
            raise ValidationError(
                f"Value out of range for {field_name}: "
                f"Expected between {min_value} and {max_value}, got {value}"
            )
    
    def validate_type(self, value: Any, expected_type: Type, field_name: str) -> None:
        """Validate that a value is of the expected type."""
        if not isinstance(value, expected_type):
            raise ValidationError(
                f"Invalid type for {field_name}: Expected {expected_type.__name__}, got {type(value).__name__}"
            )
            
    def validate_dataframe_equality(
        self,
        actual_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        ignore_index: bool = True,
        sort_by: Optional[List[str]] = None,
        check_dtype: bool = True,
        description: str = ""
    ) -> None:
        """Validate that two DataFrames are equal.
        
        Args:
            actual_df: The actual DataFrame to validate
            expected_df: The expected DataFrame to compare against
            ignore_index: Whether to ignore the index when comparing
            sort_by: Optional list of columns to sort by before comparing
            check_dtype: Whether to check data types of columns
            description: Optional description for context in error messages
        """
        context = f" for {description}" if description else ""
        
        # Sort DataFrames if sort_by is provided, otherwise use all columns
        sort_columns = sort_by if sort_by is not None else list(actual_df.columns)
        
        # Sort both DataFrames
        actual_df = actual_df.sort_values(by=sort_columns).reset_index(drop=True)
        expected_df = expected_df.sort_values(by=sort_columns).reset_index(drop=True)
        
        # Check data types if required
        if check_dtype:
            actual_dtypes = actual_df.dtypes.to_dict()
            expected_dtypes = expected_df.dtypes.to_dict()
            if actual_dtypes != expected_dtypes:
                differences = []
                for col in actual_dtypes:
                    if col in expected_dtypes and actual_dtypes[col] != expected_dtypes[col]:
                        differences.append(
                            f"Column '{col}' has mismatched types: "
                            f"Expected {expected_dtypes[col]}, got {actual_dtypes[col]}"
                        )
                raise ValidationError(
                    f"DataFrame dtypes are not equal{context}. Differences found: {', '.join(differences)}"
                )
        
        # Check if DataFrames are equal
        if not actual_df.equals(expected_df):
            # Find differences
            differences = []
            for col in actual_df.columns:
                if not actual_df[col].equals(expected_df[col]):
                    differences.append(f"Column '{col}' has mismatched values")
            
            raise ValidationError(
                f"DataFrames are not equal{context}. Differences found: {', '.join(differences)}"
            )
            
    def validate_record_count(self, actual_count: int, expected_count: int, context: str = "") -> None:
        """Validate that the actual record count matches the expected count."""
        if actual_count != expected_count:
            raise ValidationError(
                f"Record count mismatch{' for ' + context if context else ''}: "
                f"Expected {expected_count}, got {actual_count}"
            )
            
    def validate_record_exists(
        self,
        record: pd.DataFrame,
        identifier: str,
        table_name: str = "",
        context: str = ""
    ) -> None:
        """Validate that a record exists.
        
        Args:
            record: DataFrame containing the record(s)
            identifier: The identifier of the record
            table_name: Optional table name for context
            context: Additional context for error messages
        """
        if record.empty:
            table_context = f" in {table_name}" if table_name else ""
            context_str = f" for {context}" if context else ""
            raise ValidationError(
                f"Record{table_context}{context_str} with identifier '{identifier}' not found"
            )
            
    def validate_record_not_exists(
        self,
        record: pd.DataFrame,
        identifier: str,
        table_name: str = "",
        context: str = ""
    ) -> None:
        """Validate that a record does not exist.
        
        Args:
            record: DataFrame containing the record(s)
            identifier: The identifier of the record
            table_name: Optional table name for context
            context: Additional context for error messages
        """
        if not record.empty:
            table_context = f" in {table_name}" if table_name else ""
            context_str = f" for {context}" if context else ""
            raise ValidationError(
                f"Record{table_context}{context_str} with identifier '{identifier}' exists when it should not"
            ) 