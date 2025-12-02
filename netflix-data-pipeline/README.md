# 🎬 Netflix Data Engineering Pipeline

A production-grade ETL pipeline that ingests raw Netflix metadata, performs cleaning and transformation using Python and Apache Spark, and loads optimized parquet files into a query-ready data warehouse with automated validation at every stage.

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![License](https://img.shields.io/badge/License-MIT-green)

## 🌟 Key Features

### Modular Workflow Design
- **Independent Components**: Ingestion, transformation, validation, and warehouse layers
- **Scalable Architecture**: Easily handles growing datasets
- **Reusable Modules**: Plug-and-play components for different data sources

### Automated Validation Layer
- **Schema Validation**: Ensures data structure integrity
- **Quality Checks**: Completeness, uniqueness, value ranges
- **Business Rules**: Domain-specific validations
- **Automated Reports**: JSON reports with detailed metrics

### Apache Spark Processing
- **Distributed Transformations**: Handles large-scale data processing
- **Optimized Operations**: Broadcast joins, caching, partitioning
- **Advanced Enrichment**: Genre parsing, duration extraction, categorization

### Parquet Data Warehouse
- **Columnar Storage**: Optimized for analytics queries
- **Partitioning Strategy**: By release_year and type
- **Snappy Compression**: Reduced storage footprint
- **Schema Evolution**: Supports schema changes over time

## 📁 Project Structure

```
netflix-data-pipeline/
├── data/
│   ├── raw/                    # Raw input data
│   ├── staging/                # Intermediate data
│   ├── warehouse/              # Parquet files (partitioned)
│   └── reports/                # Validation reports
├── src/
│   ├── ingestion/              # Data loading modules
│   ├── transformation/         # Cleaning & Spark jobs
│   ├── validation/             # Validation framework
│   ├── warehouse/              # Parquet writer
│   ├── orchestration/          # Pipeline orchestrator
│   └── utils/                  # Logging & Spark utilities
├── config/
│   └── pipeline_config.yaml    # Pipeline configuration
├── tests/                      # Unit & integration tests
├── requirements.txt            # Python dependencies
└── run_pipeline.py             # Main entry point
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 2. Run the Pipeline

```bash
# Run with default settings
python run_pipeline.py

# Run with custom input
python run_pipeline.py --input data/raw/netflix_titles.csv --table netflix_content

# Run with custom config
python run_pipeline.py --config config/pipeline_config.yaml
```

### 3. Check the Results

```bash
# View warehouse structure
ls -R data/warehouse/

# View validation reports
cat data/reports/validation_report_*.json
```

## 📊 Pipeline Stages

### Stage 1: Data Ingestion
- Loads raw Netflix metadata (CSV/JSON/Parquet)
- Auto-detects file format and encoding
- Handles compressed files
- **Output**: Raw Spark DataFrame

### Stage 2: Raw Data Validation
- Schema validation (column types, nullability)
- Completeness checks (required fields)
- Uniqueness validation (show_id)
- Value range checks (release_year)
- **Output**: Validation report (JSON)

### Stage 3: Data Transformation
**Cleaning**:
- Remove duplicates
- Handle missing values
- Standardize dates
- Normalize text fields

**Spark Transformations**:
- Extract release_year from dates
- Parse duration into value + unit
- Split genres into arrays
- Normalize country codes
- Categorize ratings by age group
- Calculate content age
- Add recency flags

**Output**: Transformed DataFrame with 18+ columns

### Stage 4: Transformed Data Validation
- Schema validation (transformed schema)
- Business rule validation
- Statistical checks
- Allowed values verification
- **Output**: Validation report (JSON)

### Stage 5: Load to Warehouse
- Write to Parquet format
- Partition by `release_year` and `type`
- Snappy compression
- **Output**: Partitioned parquet files

### Stage 6: Reporting
- Generate pipeline summary
- Log execution metrics
- Save validation reports

## 🔧 Configuration

Edit `config/pipeline_config.yaml`:

```yaml
# Spark settings
spark:
  app_name: NetflixETL
  master: local[*]
  executor_memory: 4g
  driver_memory: 2g

# Validation settings
validation:
  fail_on_error: false      # Continue on validation errors
  max_error_rate: 0.01      # Max 1% error rate
  generate_reports: true

# Warehouse settings
warehouse:
  format: parquet
  compression: snappy
  partition_by: [release_year, type]
```

## 📈 Data Schema

### Input Schema (Raw)
- `show_id`: Unique identifier
- `type`: Movie or TV Show
- `title`: Content title
- `director`: Director name(s)
- `cast`: Cast members
- `country`: Production country
- `date_added`: Date added to Netflix
- `release_year`: Release year
- `rating`: Content rating (G, PG, R, etc.)
- `duration`: Duration string
- `listed_in`: Genres (comma-separated)
- `description`: Content description

### Output Schema (Transformed)
All input columns plus:
- `primary_country`: First country from list
- `duration_value`: Numeric duration
- `duration_unit`: 'min' or 'Season'
- `genres`: Array of genres
- `genre_count`: Number of genres
- `age_category`: Kids/Teens/Adults
- `content_age`: Years since release
- `is_recent`: Boolean flag

## 🧪 Testing

```bash
# Run unit tests
pytest tests/

# Run specific test
pytest tests/test_transformation.py

# Run with coverage
pytest --cov=src tests/
```

## 📊 Example Output

### Warehouse Structure
```
data/warehouse/netflix_content/
├── release_year=2020/
│   ├── type=Movie/
│   │   └── part-00000.parquet
│   └── type=TV Show/
│       └── part-00000.parquet
└── release_year=2021/
    ├── type=Movie/
    │   └── part-00000.parquet
    └── type=TV Show/
        └── part-00000.parquet
```

### Validation Report
```json
{
  "timestamp": "2024-12-01T23:30:00",
  "stage": "transformed",
  "summary": {
    "total_checks": 5,
    "passed_checks": 5,
    "failed_checks": 0,
    "success_rate": 1.0
  },
  "status": "PASSED"
}
```

## 🎯 Key Achievements

✅ **Modular Design**: Independent, reusable components  
✅ **Automated Validation**: Quality checks at every stage  
✅ **Scalable Processing**: Apache Spark for big data  
✅ **Optimized Storage**: Parquet with partitioning  
✅ **Production-Ready**: Logging, error handling, testing  
✅ **Well-Documented**: Comprehensive README and code comments

## 🔍 Querying the Warehouse

Use PySpark to query the warehouse:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query").getOrCreate()

# Read parquet data
df = spark.read.parquet("data/warehouse/netflix_content")

# Query examples
df.filter("type = 'Movie'").count()
df.groupBy("age_category").count().show()
df.filter("is_recent = true").select("title", "release_year").show()
```

## 🛠️ Tech Stack

- **Python 3.9+**: Core language
- **Apache Spark (PySpark 3.5)**: Distributed processing
- **Parquet**: Columnar storage format
- **PyYAML**: Configuration management
- **pytest**: Testing framework
- **pandas**: Data manipulation (ingestion)

## 📝 Future Enhancements

- [ ] Add support for incremental loads
- [ ] Implement Change Data Capture (CDC)
- [ ] Add Airflow/Prefect orchestration
- [ ] Integrate with data catalog (e.g., AWS Glue)
- [ ] Add data lineage tracking
- [ ] Implement data quality dashboards

## 👨‍💻 Author

Created by Sucheta Nandy to demonstrate:
- Data engineering expertise
- Apache Spark proficiency
- Pipeline design and orchestration
- Data quality and validation
- Production-ready code practices

## 📧 Contact

For questions about this pipeline, please reach out through the Netflix application portal.

---

**Built with ❤️ for Netflix | December 2025**
