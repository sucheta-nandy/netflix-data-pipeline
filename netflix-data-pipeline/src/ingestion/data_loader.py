"""
Data loader module for ingesting raw Netflix metadata
Supports multiple formats (CSV, JSON, Parquet) with auto-detection
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
import logging
from typing import Optional, List
import json

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles loading data from various sources and formats"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DataLoader
        
        Args:
            spark: Spark session for distributed loading
        """
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def auto_detect_format(self, path: str) -> str:
        """
        Auto-detect file format from extension
        
        Args:
            path: File path
        
        Returns:
            str: Detected format (csv, json, parquet)
        """
        path_obj = Path(path)
        extension = path_obj.suffix.lower()
        
        format_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.jsonl': 'json',
            '.parquet': 'parquet',
            '.pq': 'parquet'
        }
        
        detected_format = format_map.get(extension, 'csv')
        self.logger.info(f"Detected format: {detected_format} for file: {path}")
        return detected_format
    
    def load_csv(self, path: str, **kwargs) -> DataFrame:
        """
        Load CSV file into Spark DataFrame
        
        Args:
            path: Path to CSV file
            **kwargs: Additional options for CSV reader
        
        Returns:
            DataFrame: Loaded Spark DataFrame
        """
        self.logger.info(f"Loading CSV from: {path}")
        
        # Default options
        options = {
            'header': True,
            'inferSchema': True,
            'escape': '"',
            'multiLine': True
        }
        options.update(kwargs)
        
        try:
            df = self.spark.read.options(**options).csv(path)
            row_count = df.count()
            self.logger.info(f"Successfully loaded {row_count} rows from CSV")
            return df
        except Exception as e:
            self.logger.error(f"Error loading CSV: {str(e)}")
            raise
    
    def load_json(self, path: str, **kwargs) -> DataFrame:
        """
        Load JSON file into Spark DataFrame
        
        Args:
            path: Path to JSON file
            **kwargs: Additional options for JSON reader
        
        Returns:
            DataFrame: Loaded Spark DataFrame
        """
        self.logger.info(f"Loading JSON from: {path}")
        
        options = {
            'multiLine': True
        }
        options.update(kwargs)
        
        try:
            df = self.spark.read.options(**options).json(path)
            row_count = df.count()
            self.logger.info(f"Successfully loaded {row_count} rows from JSON")
            return df
        except Exception as e:
            self.logger.error(f"Error loading JSON: {str(e)}")
            raise
    
    def load_parquet(self, path: str) -> DataFrame:
        """
        Load Parquet file into Spark DataFrame
        
        Args:
            path: Path to Parquet file
        
        Returns:
            DataFrame: Loaded Spark DataFrame
        """
        self.logger.info(f"Loading Parquet from: {path}")
        
        try:
            df = self.spark.read.parquet(path)
            row_count = df.count()
            self.logger.info(f"Successfully loaded {row_count} rows from Parquet")
            return df
        except Exception as e:
            self.logger.error(f"Error loading Parquet: {str(e)}")
            raise
    
    def load(self, path: str, format: Optional[str] = None, **kwargs) -> DataFrame:
        """
        Load data with auto-format detection
        
        Args:
            path: Path to data file
            format: Explicit format (csv, json, parquet), auto-detected if None
            **kwargs: Additional options for the reader
        
        Returns:
            DataFrame: Loaded Spark DataFrame
        """
        if format is None:
            format = self.auto_detect_format(path)
        
        format = format.lower()
        
        if format == 'csv':
            return self.load_csv(path, **kwargs)
        elif format == 'json':
            return self.load_json(path, **kwargs)
        elif format == 'parquet':
            return self.load_parquet(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def batch_load(self, directory: str, format: str = 'csv', pattern: str = '*') -> DataFrame:
        """
        Load multiple files from a directory
        
        Args:
            directory: Directory containing files
            format: File format
            pattern: File pattern (e.g., '*.csv')
        
        Returns:
            DataFrame: Combined Spark DataFrame
        """
        self.logger.info(f"Batch loading {format} files from: {directory}")
        
        path_obj = Path(directory)
        if not path_obj.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")
        
        # Build file pattern
        if not pattern.startswith('*'):
            pattern = '*' + pattern
        
        file_pattern = str(path_obj / pattern)
        
        if format == 'csv':
            df = self.load_csv(file_pattern)
        elif format == 'json':
            df = self.load_json(file_pattern)
        elif format == 'parquet':
            df = self.load_parquet(file_pattern)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return df
