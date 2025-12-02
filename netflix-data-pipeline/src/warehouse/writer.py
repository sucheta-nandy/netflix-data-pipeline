"""
Parquet writer for data warehouse
Optimized storage with partitioning and compression
"""

from pyspark.sql import DataFrame
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ParquetWriter:
    """Handles writing data to Parquet format"""
    
    def __init__(self, base_path='data/warehouse'):
        self.base_path = Path(base_path)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def write(self, df: DataFrame, table_name: str, partition_by=None, 
              mode='overwrite', compression='snappy'):
        """
        Write DataFrame to Parquet
        
        Args:
            df: DataFrame to write
            table_name: Name of the table/dataset
            partition_by: List of columns to partition by
            mode: Write mode (overwrite, append)
            compression: Compression codec (snappy, gzip, none)
        """
        output_path = self.base_path / table_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Writing to parquet: {output_path}")
        self.logger.info(f"Partitioning by: {partition_by}")
        self.logger.info(f"Compression: {compression}")
        
        writer = df.write.mode(mode).option('compression', compression)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(str(output_path))
        
        self.logger.info(f"✅ Successfully wrote {df.count()} rows to {output_path}")
    
    def read(self, table_name: str) -> DataFrame:
        """
        Read Parquet data
        
        Args:
            table_name: Name of the table/dataset
        
        Returns:
            DataFrame: Loaded DataFrame
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        input_path = self.base_path / table_name
        
        if not input_path.exists():
            raise FileNotFoundError(f"Table not found: {input_path}")
        
        self.logger.info(f"Reading from parquet: {input_path}")
        df = spark.read.parquet(str(input_path))
        
        self.logger.info(f"✅ Successfully read {df.count()} rows from {input_path}")
        return df
