"""
Data cleaning module for Netflix metadata
Handles missing values, duplicates, and data standardization
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, 
    to_date, year, when, coalesce, lit
)
import logging

logger = logging.getLogger(__name__)


class DataCleaner:
    """Handles data cleaning operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def remove_duplicates(self, df: DataFrame, subset: list = None) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplicates (None = all columns)
        
        Returns:
            DataFrame: Deduplicated DataFrame
        """
        initial_count = df.count()
        
        if subset:
            df_clean = df.dropDuplicates(subset)
        else:
            df_clean = df.dropDuplicates()
        
        final_count = df_clean.count()
        removed = initial_count - final_count
        
        self.logger.info(f"Removed {removed} duplicate rows")
        return df_clean
    
    def handle_nulls(self, df: DataFrame, strategy='drop', fill_values=None) -> DataFrame:
        """
        Handle null values
        
        Args:
            df: Input DataFrame
            strategy: 'drop', 'fill', or 'custom'
            fill_values: Dict of column -> fill value for 'fill' strategy
        
        Returns:
            DataFrame: DataFrame with nulls handled
        """
        if strategy == 'drop':
            df_clean = df.dropna()
            self.logger.info("Dropped rows with null values")
        elif strategy == 'fill' and fill_values:
            df_clean = df.fillna(fill_values)
            self.logger.info(f"Filled nulls with: {fill_values}")
        else:
            df_clean = df
        
        return df_clean
    
    def standardize_dates(self, df: DataFrame, date_columns: list, date_format='MMMM d, yyyy') -> DataFrame:
        """
        Standardize date columns to consistent format
        
        Args:
            df: Input DataFrame
            date_columns: List of date column names
            date_format: Input date format
        
        Returns:
            DataFrame: DataFrame with standardized dates
        """
        for col_name in date_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    to_date(col(col_name), date_format)
                )
                self.logger.info(f"Standardized date column: {col_name}")
        
        return df
    
    def normalize_text(self, df: DataFrame, text_columns: list, operation='trim') -> DataFrame:
        """
        Normalize text columns
        
        Args:
            df: Input DataFrame
            text_columns: List of text column names
            operation: 'trim', 'lower', 'upper'
        
        Returns:
            DataFrame: DataFrame with normalized text
        """
        for col_name in text_columns:
            if col_name in df.columns:
                if operation == 'trim':
                    df = df.withColumn(col_name, trim(col(col_name)))
                elif operation == 'lower':
                    df = df.withColumn(col_name, lower(col(col_name)))
                elif operation == 'upper':
                    df = df.withColumn(col_name, upper(col(col_name)))
        
        self.logger.info(f"Normalized text columns with operation: {operation}")
        return df
    
    def remove_invalid_characters(self, df: DataFrame, columns: list, pattern=r'[^\w\s,.-]') -> DataFrame:
        """
        Remove invalid characters from text columns
        
        Args:
            df: Input DataFrame
            columns: List of column names
            pattern: Regex pattern for invalid characters
        
        Returns:
            DataFrame: Cleaned DataFrame
        """
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    regexp_replace(col(col_name), pattern, '')
                )
        
        self.logger.info(f"Removed invalid characters from {len(columns)} columns")
        return df
    
    def clean_netflix_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all cleaning operations specific to Netflix data
        
        Args:
            df: Raw Netflix DataFrame
        
        Returns:
            DataFrame: Cleaned DataFrame
        """
        self.logger.info("Starting Netflix data cleaning")
        
        # Remove duplicates based on show_id
        if 'show_id' in df.columns:
            df = self.remove_duplicates(df, subset=['show_id'])
        
        # Trim text columns
        text_cols = ['title', 'director', 'cast', 'country', 'listed_in', 'description']
        df = self.normalize_text(df, text_cols, operation='trim')
        
        # Standardize date_added column
        if 'date_added' in df.columns:
            df = self.standardize_dates(df, ['date_added'])
        
        # Fill missing values with appropriate defaults
        fill_values = {
            'director': 'Unknown',
            'cast': 'Unknown',
            'country': 'Unknown',
            'rating': 'Not Rated'
        }
        df = df.fillna(fill_values)
        
        self.logger.info("Netflix data cleaning completed")
        return df
