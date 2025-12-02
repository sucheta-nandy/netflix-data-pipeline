"""
Data validation framework for Netflix pipeline
Automated quality checks at every stage
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, length
import logging
from typing import Dict, List, Tuple
from .rules import NETFLIX_SCHEMA, TRANSFORMED_SCHEMA, QUALITY_RULES

logger = logging.getLogger(__name__)


class Validator:
    """Automated data validation framework"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.validation_results = []
    
    def validate_schema(self, df: DataFrame, expected_schema: Dict) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame schema against expected schema
        
        Args:
            df: Input DataFrame
            expected_schema: Expected schema definition
        
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        
        # Check for missing columns
        expected_columns = set(expected_schema.keys())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            errors.append(f"Missing columns: {missing_columns}")
        
        # Check data types
        for col_name, col_spec in expected_schema.items():
            if col_name not in df.columns:
                continue
            
            actual_type = dict(df.dtypes)[col_name]
            expected_type = col_spec.get('type')
            
            # Map Spark types to our schema types
            type_mapping = {
                'string': ['string'],
                'integer': ['int', 'bigint', 'long'],
                'double': ['double', 'float'],
                'date': ['date'],
                'boolean': ['boolean'],
                'array': ['array']
            }
            
            if expected_type in type_mapping:
                valid_types = type_mapping[expected_type]
                if not any(actual_type.startswith(vt) for vt in valid_types):
                    errors.append(
                        f"Column '{col_name}': expected {expected_type}, got {actual_type}"
                    )
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Schema validation passed")
        else:
            self.logger.warning(f"Schema validation failed: {errors}")
        
        self.validation_results.append({
            'check': 'schema_validation',
            'passed': is_valid,
            'errors': errors
        })
        
        return is_valid, errors
    
    def validate_completeness(self, df: DataFrame, required_columns: List[str]) -> Tuple[bool, Dict]:
        """
        Validate data completeness (check for nulls)
        
        Args:
            df: Input DataFrame
            required_columns: Columns that cannot be null
        
        Returns:
            Tuple of (is_valid, null_counts)
        """
        null_counts = {}
        errors = []
        
        for col_name in required_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = null_count
                
                if null_count > 0:
                    errors.append(f"Column '{col_name}' has {null_count} null values")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Completeness validation passed")
        else:
            self.logger.warning(f"Completeness validation failed: {errors}")
        
        self.validation_results.append({
            'check': 'completeness_validation',
            'passed': is_valid,
            'null_counts': null_counts,
            'errors': errors
        })
        
        return is_valid, null_counts
    
    def validate_uniqueness(self, df: DataFrame, unique_columns: List[str]) -> Tuple[bool, Dict]:
        """
        Validate uniqueness constraints
        
        Args:
            df: Input DataFrame
            unique_columns: Columns that should have unique values
        
        Returns:
            Tuple of (is_valid, duplicate_counts)
        """
        duplicate_counts = {}
        errors = []
        
        for col_name in unique_columns:
            if col_name in df.columns:
                total_count = df.count()
                distinct_count = df.select(col_name).distinct().count()
                duplicates = total_count - distinct_count
                
                duplicate_counts[col_name] = duplicates
                
                if duplicates > 0:
                    errors.append(f"Column '{col_name}' has {duplicates} duplicate values")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Uniqueness validation passed")
        else:
            self.logger.warning(f"Uniqueness validation failed: {errors}")
        
        self.validation_results.append({
            'check': 'uniqueness_validation',
            'passed': is_valid,
            'duplicate_counts': duplicate_counts,
            'errors': errors
        })
        
        return is_valid, duplicate_counts
    
    def validate_value_ranges(self, df: DataFrame, range_rules: Dict) -> Tuple[bool, Dict]:
        """
        Validate value ranges
        
        Args:
            df: Input DataFrame
            range_rules: Dict of column -> {min, max} rules
        
        Returns:
            Tuple of (is_valid, violation_counts)
        """
        violation_counts = {}
        errors = []
        
        for col_name, rules in range_rules.items():
            if col_name not in df.columns:
                continue
            
            violations = 0
            
            if 'min' in rules:
                min_violations = df.filter(col(col_name) < rules['min']).count()
                violations += min_violations
                if min_violations > 0:
                    errors.append(
                        f"Column '{col_name}': {min_violations} values below min ({rules['min']})"
                    )
            
            if 'max' in rules:
                max_violations = df.filter(col(col_name) > rules['max']).count()
                violations += max_violations
                if max_violations > 0:
                    errors.append(
                        f"Column '{col_name}': {max_violations} values above max ({rules['max']})"
                    )
            
            violation_counts[col_name] = violations
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Value range validation passed")
        else:
            self.logger.warning(f"Value range validation failed: {errors}")
        
        self.validation_results.append({
            'check': 'value_range_validation',
            'passed': is_valid,
            'violation_counts': violation_counts,
            'errors': errors
        })
        
        return is_valid, violation_counts
    
    def validate_allowed_values(self, df: DataFrame, allowed_values_rules: Dict) -> Tuple[bool, Dict]:
        """
        Validate that column values are in allowed set
        
        Args:
            df: Input DataFrame
            allowed_values_rules: Dict of column -> list of allowed values
        
        Returns:
            Tuple of (is_valid, violation_counts)
        """
        violation_counts = {}
        errors = []
        
        for col_name, allowed_values in allowed_values_rules.items():
            if col_name not in df.columns:
                continue
            
            violations = df.filter(~col(col_name).isin(allowed_values)).count()
            violation_counts[col_name] = violations
            
            if violations > 0:
                errors.append(
                    f"Column '{col_name}': {violations} values not in allowed set"
                )
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Allowed values validation passed")
        else:
            self.logger.warning(f"Allowed values validation failed: {errors}")
        
        self.validation_results.append({
            'check': 'allowed_values_validation',
            'passed': is_valid,
            'violation_counts': violation_counts,
            'errors': errors
        })
        
        return is_valid, violation_counts
    
    def validate_netflix_raw_data(self, df: DataFrame) -> bool:
        """
        Validate raw Netflix data
        
        Args:
            df: Raw Netflix DataFrame
        
        Returns:
            bool: Whether validation passed
        """
        self.logger.info("Starting raw data validation")
        self.validation_results = []
        
        # Schema validation
        schema_valid, _ = self.validate_schema(df, NETFLIX_SCHEMA)
        
        # Completeness validation
        required_cols = QUALITY_RULES['completeness']['required_columns']
        completeness_valid, _ = self.validate_completeness(df, required_cols)
        
        # Uniqueness validation
        unique_cols = QUALITY_RULES['uniqueness']['unique_columns']
        uniqueness_valid, _ = self.validate_uniqueness(df, unique_cols)
        
        # Value range validation
        range_rules = {
            'release_year': {'min': 1900, 'max': 2025}
        }
        range_valid, _ = self.validate_value_ranges(df, range_rules)
        
        all_valid = all([schema_valid, completeness_valid, uniqueness_valid, range_valid])
        
        if all_valid:
            self.logger.info("✅ Raw data validation PASSED")
        else:
            self.logger.warning("❌ Raw data validation FAILED")
        
        return all_valid
    
    def validate_transformed_data(self, df: DataFrame) -> bool:
        """
        Validate transformed Netflix data
        
        Args:
            df: Transformed Netflix DataFrame
        
        Returns:
            bool: Whether validation passed
        """
        self.logger.info("Starting transformed data validation")
        self.validation_results = []
        
        # Schema validation (subset of transformed schema)
        transformed_cols = {k: v for k, v in TRANSFORMED_SCHEMA.items() if k in df.columns}
        schema_valid, _ = self.validate_schema(df, transformed_cols)
        
        # Completeness validation
        required_cols = ['show_id', 'type', 'title', 'release_year', 'duration_value']
        completeness_valid, _ = self.validate_completeness(df, required_cols)
        
        # Value range validation
        range_rules = {
            'release_year': {'min': 1900, 'max': 2025},
            'duration_value': {'min': 1, 'max': 1000},
            'genre_count': {'min': 1, 'max': 10},
            'content_age': {'min': 0, 'max': 125}
        }
        range_valid, _ = self.validate_value_ranges(df, range_rules)
        
        # Allowed values validation
        allowed_values_rules = {
            'type': ['Movie', 'TV Show'],
            'duration_unit': ['min', 'Season', 'Unknown'],
            'age_category': ['Kids', 'Older Kids', 'Teens', 'Adults', 'Not Rated']
        }
        allowed_valid, _ = self.validate_allowed_values(df, allowed_values_rules)
        
        all_valid = all([schema_valid, completeness_valid, range_valid, allowed_valid])
        
        if all_valid:
            self.logger.info("✅ Transformed data validation PASSED")
        else:
            self.logger.warning("❌ Transformed data validation FAILED")
        
        return all_valid
    
    def get_validation_summary(self) -> Dict:
        """
        Get summary of all validation results
        
        Returns:
            Dict: Validation summary
        """
        total_checks = len(self.validation_results)
        passed_checks = sum(1 for r in self.validation_results if r['passed'])
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': total_checks - passed_checks,
            'success_rate': passed_checks / total_checks if total_checks > 0 else 0,
            'details': self.validation_results
        }
