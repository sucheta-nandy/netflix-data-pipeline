"""
Apache Spark utility functions
Provides helpers for Spark session management and DataFrame operations
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import yaml
from pathlib import Path


def create_spark_session(app_name="NetflixETL", config_path=None):
    """
    Create and configure a Spark session
    
    Args:
        app_name: Name of the Spark application
        config_path: Path to configuration file (optional)
    
    Returns:
        SparkSession: Configured Spark session
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Load configuration if provided
    if config_path:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            spark_config = config.get('spark', {})
            
            if 'master' in spark_config:
                builder = builder.master(spark_config['master'])
            if 'executor_memory' in spark_config:
                builder = builder.config('spark.executor.memory', spark_config['executor_memory'])
            if 'driver_memory' in spark_config:
                builder = builder.config('spark.driver.memory', spark_config['driver_memory'])
            if 'shuffle_partitions' in spark_config:
                builder = builder.config('spark.sql.shuffle.partitions', spark_config['shuffle_partitions'])
    
    # Additional optimizations
    builder = builder.config('spark.sql.adaptive.enabled', 'true')
    builder = builder.config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    
    spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel('WARN')
    
    return spark


def profile_dataframe(df: DataFrame, sample_size=5):
    """
    Generate a profile of a Spark DataFrame
    
    Args:
        df: Spark DataFrame to profile
        sample_size: Number of sample rows to show
    
    Returns:
        dict: Profile information
    """
    profile = {
        'row_count': df.count(),
        'column_count': len(df.columns),
        'columns': df.columns,
        'schema': df.schema.simpleString(),
        'sample_data': df.limit(sample_size).toPandas().to_dict('records')
    }
    
    return profile


def get_null_counts(df: DataFrame):
    """
    Get null counts for all columns in a DataFrame
    
    Args:
        df: Spark DataFrame
    
    Returns:
        dict: Column name to null count mapping
    """
    from pyspark.sql.functions import sum as spark_sum, when, col
    
    null_counts = df.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    return null_counts


def optimize_dataframe(df: DataFrame, num_partitions=None):
    """
    Optimize a DataFrame by repartitioning
    
    Args:
        df: Spark DataFrame
        num_partitions: Target number of partitions (optional)
    
    Returns:
        DataFrame: Optimized DataFrame
    """
    if num_partitions:
        return df.repartition(num_partitions)
    
    # Auto-calculate based on data size
    row_count = df.count()
    if row_count < 10000:
        return df.coalesce(1)
    elif row_count < 100000:
        return df.coalesce(4)
    else:
        return df.repartition(8)


def cache_dataframe(df: DataFrame, storage_level='MEMORY_AND_DISK'):
    """
    Cache a DataFrame with specified storage level
    
    Args:
        df: Spark DataFrame
        storage_level: Storage level for caching
    
    Returns:
        DataFrame: Cached DataFrame
    """
    from pyspark import StorageLevel
    
    storage_map = {
        'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
        'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
        'DISK_ONLY': StorageLevel.DISK_ONLY
    }
    
    level = storage_map.get(storage_level, StorageLevel.MEMORY_AND_DISK)
    return df.persist(level)


def write_parquet(df: DataFrame, path, partition_by=None, mode='overwrite', compression='snappy'):
    """
    Write DataFrame to Parquet format with optimizations
    
    Args:
        df: Spark DataFrame
        path: Output path
        partition_by: List of columns to partition by
        mode: Write mode (overwrite, append, etc.)
        compression: Compression codec
    """
    writer = df.write.mode(mode).option('compression', compression)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.parquet(path)


def read_parquet(spark: SparkSession, path):
    """
    Read Parquet files into a DataFrame
    
    Args:
        spark: Spark session
        path: Path to parquet files
    
    Returns:
        DataFrame: Loaded DataFrame
    """
    return spark.read.parquet(path)
