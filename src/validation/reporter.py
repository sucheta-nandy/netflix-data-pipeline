"""
Validation reporter - generates data quality reports
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class ValidationReporter:
    """Generates data quality reports"""
    
    def __init__(self, output_dir='data/reports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate_report(self, validation_summary: Dict, stage: str = 'unknown') -> str:
        """
        Generate validation report
        
        Args:
            validation_summary: Validation results summary
            stage: Pipeline stage (raw, transformed, etc.)
        
        Returns:
            str: Path to generated report
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_name = f'validation_report_{stage}_{timestamp}.json'
        report_path = self.output_dir / report_name
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'stage': stage,
            'summary': validation_summary,
            'status': 'PASSED' if validation_summary['failed_checks'] == 0 else 'FAILED'
        }
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Generated validation report: {report_path}")
        return str(report_path)
    
    def print_summary(self, validation_summary: Dict):
        """Print validation summary to console"""
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        print(f"Total Checks: {validation_summary['total_checks']}")
        print(f"Passed: {validation_summary['passed_checks']}")
        print(f"Failed: {validation_summary['failed_checks']}")
        print(f"Success Rate: {validation_summary['success_rate']:.1%}")
        print("="*60 + "\n")
