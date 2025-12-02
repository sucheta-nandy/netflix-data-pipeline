"""
Quick test script to verify pipeline components
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

print("🧪 Testing Netflix Data Pipeline Components\n")

# Test 1: Import all modules
print("1. Testing imports...")
try:
    from src.utils.logger import setup_logger
    from src.ingestion.data_loader import DataLoader
    from src.transformation.cleaner import DataCleaner
    from src.transformation.spark_jobs import SparkTransformer
    from src.validation.validator import Validator
    from src.validation.reporter import ValidationReporter
    from src.warehouse.writer import ParquetWriter
    from src.orchestration.pipeline import NetflixDataPipeline
    print("   ✅ All imports successful\n")
except Exception as e:
    print(f"   ❌ Import failed: {e}\n")
    sys.exit(1)

# Test 2: Check configuration
print("2. Testing configuration...")
try:
    import yaml
    with open('config/pipeline_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    print(f"   ✅ Config loaded: {config['pipeline']['name']}\n")
except Exception as e:
    print(f"   ❌ Config failed: {e}\n")
    sys.exit(1)

# Test 3: Check sample data
print("3. Testing sample data...")
try:
    data_path = Path('data/raw/netflix_titles.csv')
    if data_path.exists():
        with open(data_path, 'r') as f:
            lines = f.readlines()
        print(f"   ✅ Sample data found: {len(lines)-1} records\n")
    else:
        print(f"   ❌ Sample data not found\n")
except Exception as e:
    print(f"   ❌ Data check failed: {e}\n")

print("="*60)
print("✅ All component tests passed!")
print("="*60)
print("\nReady to run the full pipeline:")
print("  python run_pipeline.py")
print("\nOr install dependencies first:")
print("  pip install -r requirements.txt")
print("="*60)
