#!/usr/bin/env python3
"""
Netflix Data Pipeline - Main Entry Point
Run the complete ETL pipeline
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.orchestration.pipeline import NetflixDataPipeline


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Netflix Data Engineering Pipeline'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='data/raw/netflix_titles.csv',
        help='Path to input data file'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--table',
        type=str,
        default='netflix_content',
        help='Output table name'
    )
    
    args = parser.parse_args()
    
    # Initialize and run pipeline
    print("\n🎬 Netflix Data Engineering Pipeline")
    print("="*60)
    
    pipeline = NetflixDataPipeline(config_path=args.config)
    
    try:
        success = pipeline.run(
            input_path=args.input,
            table_name=args.table
        )
        
        if success:
            print("\n✅ Pipeline completed successfully!")
            return 0
        else:
            print("\n❌ Pipeline failed!")
            return 1
    
    finally:
        pipeline.stop()


if __name__ == '__main__':
    sys.exit(main())
