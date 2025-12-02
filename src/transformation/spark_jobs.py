"""
Spark transformation jobs for Netflix metadata
Distributed data processing using Apache Spark
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, split, explode, trim, year, regexp_extract,
    when, lit, array_distinct, size, datediff, current_date,
    concat_ws, lower, upper
)
import logging

logger = logging.getLogger(__name__)


class SparkTransformer:
    """Handles Spark-based data transformations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_release_year(self, df: DataFrame) -> DataFrame:
        """
        Extract release year from date_added if not present
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame: DataFrame with release_year
        """
        if 'release_year' not in df.columns and 'date_added' in df.columns:
            df = df.withColumn('release_year', year(col('date_added')))
            self.logger.info("Extracted release_year from date_added")
        
        return df
    
    def parse_duration(self, df: DataFrame) -> DataFrame:
        """
        Parse duration column into numeric values
        
        Args:
            df: Input DataFrame with 'duration' column
        
        Returns:
            DataFrame: DataFrame with duration_value and duration_unit
        """
        if 'duration' in df.columns:
            # Extract numeric value
            df = df.withColumn(
                'duration_value',
                regexp_extract(col('duration'), r'(\d+)', 1).cast('integer')
            )
            
            # Extract unit (min or Season)
            df = df.withColumn(
                'duration_unit',
                when(col('duration').contains('Season'), 'Season')
                .when(col('duration').contains('min'), 'min')
                .otherwise('Unknown')
            )
            
            self.logger.info("Parsed duration into value and unit")
        
        return df
    
    def parse_genres(self, df: DataFrame) -> DataFrame:
        """
        Parse listed_in column into array of genres
        
        Args:
            df: Input DataFrame with 'listed_in' column
        
        Returns:
            DataFrame: DataFrame with genres array
        """
        if 'listed_in' in df.columns:
            # Split by comma and trim whitespace
            df = df.withColumn(
                'genres',
                split(col('listed_in'), ',')
            )
            
            # Trim each genre
            df = df.withColumn(
                'genres',
                array_distinct(
                    split(regexp_replace(col('listed_in'), r'\s*,\s*', ','), ',')
                )
            )
            
            # Add genre count
            df = df.withColumn('genre_count', size(col('genres')))
            
            self.logger.info("Parsed genres from listed_in")
        
        return df
    
    def normalize_country(self, df: DataFrame) -> DataFrame:
        """
        Normalize country column (take first country if multiple)
        
        Args:
            df: Input DataFrame with 'country' column
        
        Returns:
            DataFrame: DataFrame with primary_country
        """
        if 'country' in df.columns:
            df = df.withColumn(
                'primary_country',
                trim(split(col('country'), ',').getItem(0))
            )
            
            self.logger.info("Normalized country to primary_country")
        
        return df
    
    def categorize_rating(self, df: DataFrame) -> DataFrame:
        """
        Categorize ratings into age groups
        
        Args:
            df: Input DataFrame with 'rating' column
        
        Returns:
            DataFrame: DataFrame with age_category
        """
        if 'rating' in df.columns:
            df = df.withColumn(
                'age_category',
                when(col('rating').isin(['G', 'TV-Y', 'TV-G']), 'Kids')
                .when(col('rating').isin(['PG', 'TV-Y7', 'TV-PG']), 'Older Kids')
                .when(col('rating').isin(['PG-13', 'TV-14']), 'Teens')
                .when(col('rating').isin(['R', 'TV-MA', 'NC-17']), 'Adults')
                .otherwise('Not Rated')
            )
            
            self.logger.info("Categorized ratings into age groups")
        
        return df
    
    def add_content_age(self, df: DataFrame) -> DataFrame:
        """
        Calculate content age (years since release)
        
        Args:
            df: Input DataFrame with 'release_year' column
        
        Returns:
            DataFrame: DataFrame with content_age
        """
        if 'release_year' in df.columns:
            from datetime import datetime
            current_year = datetime.now().year
            
            df = df.withColumn(
                'content_age',
                lit(current_year) - col('release_year')
            )
            
            self.logger.info("Added content_age column")
        
        return df
    
    def add_is_recent(self, df: DataFrame, threshold_years=3) -> DataFrame:
        """
        Add flag for recent content
        
        Args:
            df: Input DataFrame
            threshold_years: Years threshold for "recent"
        
        Returns:
            DataFrame: DataFrame with is_recent flag
        """
        if 'content_age' in df.columns:
            df = df.withColumn(
                'is_recent',
                when(col('content_age') <= threshold_years, True).otherwise(False)
            )
            
            self.logger.info(f"Added is_recent flag (threshold: {threshold_years} years)")
        
        return df
    
    def transform_netflix_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to Netflix data
        
        Args:
            df: Cleaned Netflix DataFrame
        
        Returns:
            DataFrame: Fully transformed DataFrame
        """
        self.logger.info("Starting Spark transformations")
        
        # Extract and enrich data
        df = self.extract_release_year(df)
        df = self.parse_duration(df)
        df = self.parse_genres(df)
        df = self.normalize_country(df)
        df = self.categorize_rating(df)
        df = self.add_content_age(df)
        df = self.add_is_recent(df)
        
        # Select and reorder columns
        final_columns = [
            'show_id', 'type', 'title', 'director', 'cast',
            'primary_country', 'date_added', 'release_year',
            'rating', 'age_category', 'duration', 'duration_value',
            'duration_unit', 'genres', 'genre_count', 'description',
            'content_age', 'is_recent'
        ]
        
        # Only select columns that exist
        available_columns = [c for c in final_columns if c in df.columns]
        df = df.select(*available_columns)
        
        self.logger.info("Spark transformations completed")
        return df
