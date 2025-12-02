# 🚀 Deployment Guide

## Get a Public URL for Your Netflix Data Pipeline

You have **3 easy options** to host this project online:

---

## Option 1: GitHub (Recommended) ⭐

**Why**: Shows your Git proficiency, professional URL, easy to share

### Quick Deploy Script
```bash
cd /Users/suchetanandy/.gemini/antigravity/scratch/netflix-data-pipeline
./deploy.sh
```

The script will guide you through:
1. Creating a GitHub repository
2. Pushing your code
3. Getting your public URL

**Your URL will be**: `https://github.com/YOUR_USERNAME/netflix-data-pipeline`

### Manual Steps (Alternative)

1. **Create GitHub Repository**
   - Go to https://github.com/new
   - Name: `netflix-data-pipeline`
   - Description: "Production-grade ETL pipeline with Apache Spark and automated validation"
   - Make it **PUBLIC**
   - Don't initialize with README
   - Click "Create repository"

2. **Push Your Code**
   ```bash
   cd /Users/suchetanandy/.gemini/antigravity/scratch/netflix-data-pipeline
   
   # Add your repository (replace YOUR_USERNAME)
   git remote add origin https://github.com/YOUR_USERNAME/netflix-data-pipeline.git
   
   # Push to GitHub
   git branch -M main
   git push -u origin main
   ```

3. **Get Your URL**
   ```
   https://github.com/YOUR_USERNAME/netflix-data-pipeline
   ```

---

## Option 2: GitLab

Similar to GitHub but on GitLab:

1. Go to https://gitlab.com/projects/new
2. Create project: `netflix-data-pipeline`
3. Push code:
   ```bash
   git remote add origin https://gitlab.com/YOUR_USERNAME/netflix-data-pipeline.git
   git push -u origin main
   ```

**URL**: `https://gitlab.com/YOUR_USERNAME/netflix-data-pipeline`

---

## Option 3: Bitbucket

1. Go to https://bitbucket.org/repo/create
2. Create repository: `netflix-data-pipeline`
3. Push code:
   ```bash
   git remote add origin https://YOUR_USERNAME@bitbucket.org/YOUR_USERNAME/netflix-data-pipeline.git
   git push -u origin main
   ```

**URL**: `https://bitbucket.org/YOUR_USERNAME/netflix-data-pipeline`

---

## For Your Netflix Application

### Include This URL

**GitHub**: `https://github.com/YOUR_USERNAME/netflix-data-pipeline`

### Highlight These Features

```
Production-Grade Data Engineering Pipeline

Key Features:
✅ Modular ETL architecture with Apache Spark
✅ Automated validation framework (schema, quality, business rules)
✅ Parquet data warehouse with partitioning strategy
✅ Comprehensive logging and error handling
✅ 2,500+ lines of production-ready Python code

Technologies:
- Apache Spark (PySpark 3.5) for distributed processing
- Parquet with Snappy compression
- YAML configuration management
- pytest for testing
- Modular, scalable architecture

Pipeline Stages:
1. Ingestion (CSV/JSON/Parquet with auto-detection)
2. Raw Data Validation
3. Transformation (cleaning, enrichment, normalization)
4. Transformed Data Validation
5. Warehouse (partitioned parquet files)
6. Quality Reporting

This demonstrates my expertise in:
- Data engineering and ETL design
- Apache Spark for big data processing
- Data quality and validation frameworks
- Production-ready code with proper logging
- Modular, maintainable architecture
```

---

## Troubleshooting

### Authentication Issues

If you have 2FA enabled on GitHub:
1. Go to https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Select scopes: `repo`
4. Copy the token
5. Use the token as your password when pushing

### Repository Already Exists

```bash
# Remove existing remote
git remote remove origin

# Add new remote
git remote add origin https://github.com/YOUR_USERNAME/netflix-data-pipeline.git

# Push
git push -u origin main
```

---

## Quick Reference

**Project Location**: `/Users/suchetanandy/.gemini/antigravity/scratch/netflix-data-pipeline/`

**Deploy Command**: `./deploy.sh`

**Files Committed**: 29 files, 2,500+ lines of code

**Ready to share with Netflix recruiters! 🚀**
