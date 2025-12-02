"""
Main Netflix ETL Pipeline Orchestrator
Coordinates all stages of the data pipeline with automated validation
"""

import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession

from ..utils.spark_utils import create_spark_session
from ..utils.logger import setup_logger
from ..ingestion.data_loader import DataLoader
from ..transformation.cleaner import DataCleaner
from ..transformation.spark_jobs import SparkTransformer
from ..validation.validator import Validator
from ..validation.reporter import ValidationReporter
from ..warehouse.writer import ParquetWriter


class NetflixDataPipeline:
    """
    Main pipeline orchestrator for Netflix ETL
    
    Stages:
    1. Ingestion - Load raw data
    2. Validation (Raw) - Validate raw data quality
    3. Transformation - Clean and enrich data
    4. Validation (Transformed) - Validate transformed data
    5. Warehouse - Write to parquet with partitioning
    6. Reporting - Generate quality reports
    """
    
    def __init__(self, config_path='config/pipeline_config.yaml'):
        """
        Initialize pipeline
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Setup logging
        log_config = self.config.get('logging', {})
        self.logger = setup_logger(
            'NetflixPipeline',
            log_file=log_config.get('file', 'logs/pipeline.log'),
            level=getattr(logging, log_config.get('level', 'INFO'))
        )
        
        # Initialize Spark
        self.logger.info("Initializing Spark session")
        self.spark = create_spark_session(
            app_name=self.config['spark']['app_name'],
            config_path=config_path
        )
        
        # Initialize components
        self.loader = DataLoader(self.spark)
        self.cleaner = DataCleaner()
        self.transformer = SparkTransformer()
        self.validator = Validator()
        self.reporter = ValidationReporter(
            output_dir=self.config['validation'].get('report_path', 'data/reports')
        )
        self.writer = ParquetWriter(
            base_path=self.config['data']['warehouse_path']
        )
        
        self.logger.info("Pipeline initialized successfully")
    
    def run(self, input_path: str, table_name='netflix_content'):
        """
        Run the complete ETL pipeline
        
        Args:
            input_path: Path to raw data file
            table_name: Name for the warehouse table
        """
        self.logger.info("="*60)
        self.logger.info("STARTING NETFLIX ETL PIPELINE")
        self.logger.info("="*60)
        
        try:
            # Stage 1: Ingestion
            self.logger.info("\n📥 Stage 1: Data Ingestion")
            raw_data = self.ingest_data(input_path)
            
            # Stage 2: Raw Data Validation
            self.logger.info("\n✅ Stage 2: Raw Data Validation")
            self.validate_raw_data(raw_data)
            
            # Stage 3: Transformation
            self.logger.info("\n🔄 Stage 3: Data Transformation")
            transformed_data = self.transform_data(raw_data)
            
            # Stage 4: Transformed Data Validation
            self.logger.info("\n✅ Stage 4: Transformed Data Validation")
            self.validate_transformed_data(transformed_data)
            
            # Stage 5: Load to Warehouse
            self.logger.info("\n💾 Stage 5: Load to Warehouse")
            self.load_to_warehouse(transformed_data, table_name)
            
            # Stage 6: Generate Reports
            self.logger.info("\n📊 Stage 6: Generate Reports")
            self.generate_reports()
            
            self.logger.info("\n" + "="*60)
            self.logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"\n❌ PIPELINE FAILED: {str(e)}", exc_info=True)
            return False
        
        finally:
            # Cleanup
            self.logger.info("\nCleaning up resources")
            # Spark session will be stopped by the caller if needed
    
    def ingest_data(self, input_path: str):
        """
        Ingest raw data
        
        Args:
            input_path: Path to input file
        
        Returns:
            DataFrame: Raw data
        """
        self.logger.info(f"Loading data from: {input_path}")
        df = self.loader.load(input_path)
        
        row_count = df.count()
        col_count = len(df.columns)
        
        self.logger.info(f"Loaded {row_count} rows, {col_count} columns")
        self.logger.info(f"Columns: {df.columns}")
        
        return df
    
    def validate_raw_data(self, df):
        """
        Validate raw data
        
        Args:
            df: Raw DataFrame
        """
        is_valid = self.validator.validate_netflix_raw_data(df)
        
        # Get validation summary
        summary = self.validator.get_validation_summary()
        self.reporter.print_summary(summary)
        
        # Generate report
        report_path = self.reporter.generate_report(summary, stage='raw')
        self.logger.info(f"Validation report saved to: {report_path}")
        
        # Check if we should fail on validation errors
        if not is_valid and self.config['validation'].get('fail_on_error', False):
            raise ValueError("Raw data validation failed")
    
    def transform_data(self, df):
        """
        Transform data
        
        Args:
            df: Raw DataFrame
        
        Returns:
            DataFrame: Transformed DataFrame
        """
        # Clean data
        self.logger.info("Cleaning data...")
        df_clean = self.cleaner.clean_netflix_data(df)
        
        # Transform with Spark
        self.logger.info("Applying Spark transformations...")
        df_transformed = self.transformer.transform_netflix_data(df_clean)
        
        # Cache for multiple validations
        df_transformed.cache()
        
        row_count = df_transformed.count()
        col_count = len(df_transformed.columns)
        
        self.logger.info(f"Transformed data: {row_count} rows, {col_count} columns")
        
        return df_transformed
    
    def validate_transformed_data(self, df):
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
        """
        is_valid = self.validator.validate_transformed_data(df)
        
        # Get validation summary
        summary = self.validator.get_validation_summary()
        self.reporter.print_summary(summary)
        
        # Generate report
        report_path = self.reporter.generate_report(summary, stage='transformed')
        self.logger.info(f"Validation report saved to: {report_path}")
        
        # Check if we should fail on validation errors
        if not is_valid and self.config['validation'].get('fail_on_error', False):
            raise ValueError("Transformed data validation failed")
    
    def load_to_warehouse(self, df, table_name):
        """
        Load data to warehouse
        
        Args:
            df: Transformed DataFrame
            table_name: Table name
        """
        warehouse_config = self.config['warehouse']
        
        self.writer.write(
            df,
            table_name=table_name,
            partition_by=warehouse_config.get('partition_by'),
            mode=warehouse_config.get('mode', 'overwrite'),
            compression=warehouse_config.get('compression', 'snappy')
        )
        
        self.logger.info(f"✅ Data loaded to warehouse table: {table_name}")
    
    def generate_reports(self):
        """Generate final pipeline reports"""
        self.logger.info("Pipeline execution completed")
        self.logger.info(f"Warehouse location: {self.config['data']['warehouse_path']}")
        self.logger.info(f"Reports location: {self.config['validation'].get('report_path')}")
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.logger.info("Stopping Spark session")
            self.spark.stop()
