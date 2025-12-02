# 🚀 Netflix Data Engineering Pipeline - Quick Start Guide

## Project Location
```
/Users/suchetanandy/.gemini/antigravity/scratch/netflix-data-pipeline/
```

## Setup Instructions

### 1. Navigate to Project
```bash
cd /Users/suchetanandy/.gemini/antigravity/scratch/netflix-data-pipeline
```

### 2. Install Dependencies
```bash
# Option A: Install directly (if you have pip)
pip3 install -r requirements.txt

# Option B: Use virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Pipeline
```bash
python3 run_pipeline.py
```

## What Gets Created

### Input
- `data/raw/netflix_titles.csv` - Sample Netflix metadata (10 records)

### Output
- `data/warehouse/netflix_content/` - Partitioned parquet files
  - Partitioned by `release_year` and `type`
  - Snappy compressed
  - Optimized for analytics

- `data/reports/` - Validation reports (JSON)
  - Raw data validation results
  - Transformed data validation results

- `logs/pipeline.log` - Execution logs (JSON format)

## Pipeline Stages

1. **Ingestion** → Load CSV data
2. **Raw Validation** → Check schema & quality
3. **Transformation** → Clean & enrich with Spark
4. **Transformed Validation** → Verify output quality
5. **Warehouse** → Write partitioned parquet
6. **Reporting** → Generate quality reports

## Key Features Demonstrated

✅ **Modular Architecture**
- Independent ingestion, transformation, validation layers
- Easy to extend and maintain

✅ **Automated Validation**
- Schema validation
- Completeness checks
- Value range validation
- Business rule enforcement

✅ **Apache Spark Processing**
- Genre parsing into arrays
- Duration extraction
- Country normalization
- Age categorization
- Content enrichment

✅ **Optimized Warehouse**
- Parquet columnar format
- Partitioning by release_year + type
- Snappy compression
- Query-ready structure

## Project Structure

```
netflix-data-pipeline/
├── src/
│   ├── ingestion/          # Data loading (CSV/JSON/Parquet)
│   ├── transformation/     # Cleaning + Spark jobs
│   ├── validation/         # Quality framework
│   ├── warehouse/          # Parquet writer
│   ├── orchestration/      # Pipeline orchestrator
│   └── utils/              # Logging + Spark helpers
├── data/
│   ├── raw/                # Input data
│   ├── warehouse/          # Parquet output
│   └── reports/            # Validation reports
├── config/
│   └── pipeline_config.yaml
├── run_pipeline.py         # Main entry point
├── requirements.txt        # Dependencies
└── README.md               # Full documentation
```

## Dependencies

- **pyspark==3.5.0** - Apache Spark for distributed processing
- **pandas==2.1.4** - Data manipulation
- **pyyaml==6.0.1** - Configuration management
- **pytest==7.4.3** - Testing framework
- **pyarrow==14.0.1** - Parquet support

## Example Output

After running the pipeline, you'll see:

```
🎬 Netflix Data Engineering Pipeline
============================================================

📥 Stage 1: Data Ingestion
Loaded 10 rows, 12 columns

✅ Stage 2: Raw Data Validation
============================================================
VALIDATION SUMMARY
============================================================
Total Checks: 4
Passed: 4
Failed: 0
Success Rate: 100.0%
============================================================

🔄 Stage 3: Data Transformation
Cleaning data...
Applying Spark transformations...
Transformed data: 10 rows, 18 columns

✅ Stage 4: Transformed Data Validation
[Similar validation summary]

💾 Stage 5: Load to Warehouse
Writing to parquet: data/warehouse/netflix_content
Partitioning by: ['release_year', 'type']
✅ Successfully wrote 10 rows

📊 Stage 6: Generate Reports
Pipeline execution completed

============================================================
✅ PIPELINE COMPLETED SUCCESSFULLY
============================================================
```

## Querying the Warehouse

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query").getOrCreate()
df = spark.read.parquet("data/warehouse/netflix_content")

# Show schema
df.printSchema()

# Count by type
df.groupBy("type").count().show()

# Filter recent movies
df.filter("type = 'Movie' AND is_recent = true").show()

# Genre analysis
df.groupBy("age_category").count().show()
```

## Troubleshooting

### "No module named 'pyspark'"
```bash
pip3 install pyspark==3.5.0
```

### "Java not found"
PySpark requires Java. Install:
```bash
# macOS
brew install openjdk@11

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

### Permission denied
```bash
chmod +x run_pipeline.py
```

## Next Steps

1. **Extend the pipeline**:
   - Add more transformation logic
   - Implement incremental loads
   - Add more validation rules

2. **Scale up**:
   - Use larger datasets
   - Deploy to cluster (EMR, Databricks)
   - Add orchestration (Airflow)

3. **Integrate**:
   - Connect to data catalog
   - Add BI tool integration
   - Implement data lineage

---

**Ready to showcase production-grade data engineering skills! 🚀**
