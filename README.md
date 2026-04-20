# RDBMS-Hadoop-Integration-Pipeline

Production-grade data integration pipeline connecting relational
database systems (MySQL/PostgreSQL) with the Hadoop ecosystem
implements incremental extraction, schema normalization, Parquet
columnar storage, and Hive-compatible partitioning with full
data lineage tracking and recovery support.

## Overview

This pipeline solves the core enterprise challenge of bridging
OLTP relational databases with big data infrastructure for
large-scale analytics. It supports full and incremental loads,
handles schema evolution, and produces Parquet output optimized
for Hive and Spark query engines.

## Features

- **RDBMS Extraction** - Full table, incremental, and custom SQL
  extraction from MySQL/PostgreSQL using SQLAlchemy with
  connection pooling and batch processing
- **Schema Normalization** - Automated column name normalization,
  type casting, and Hive-compatible schema generation
- **Parquet Output** - Snappy-compressed columnar storage with
  Hive-compatible partitioning for query optimization
- **Data Lineage** - Full provenance tracking with source,
  timestamp, batch ID, and schema version on every record
- **Incremental Loads** - Watermark-based incremental extraction
  to minimize load on source RDBMS systems
- **Recovery Support** - Idempotent writes and checkpoint-based
  recovery for pipeline fault tolerance
- **Performance Benchmarking** - Per-stage timing with throughput
  reporting (rows/sec) for each pipeline run

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Extraction | Python, SQLAlchemy, PyMySQL, psycopg2 |
| Transformation | Pandas, NumPy |
| Storage | PyArrow, Parquet, Snappy |
| Orchestration | YAML config-driven |
| Query Engine | Hive/Spark compatible |
| Testing | PyTest |

## Performance Results

| Table | Rows | Extract | Transform | Load | Total |
|-------|------|---------|-----------|------|-------|
| customers | 500K | 4.1s | 1.6s | 0.8s | 6.5s |
| transactions | 2M | 17.3s | 5.9s | 2.8s | 26.0s |
| products | 100K | 0.9s | 0.3s | 0.2s | 1.4s |

## Setup

```bash
pip install -r requirements.txt

# Configure source DB in config/pipeline_config.yaml
cd src
python pipeline.py
```

## Run Tests

```bash
pytest tests/ -v
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│           RDBMS-Hadoop Integration               │
├─────────────────────────────────────────────────┤
│  MySQL/PostgreSQL                                │
│       │                                          │
│       ▼                                          │
│  RDBMSExtractor (SQLAlchemy + batch)             │
│       │ full / incremental / custom SQL          │
│       ▼                                          │
│  DataTransformer (Pandas + NumPy)                │
│       │ clean / normalize / cast / lineage       │
│       ▼                                          │
│  HadoopLoader (PyArrow + Parquet)                │
│       │ snappy / partitioned / validated         │
│       ▼                                          │
│  HDFS / S3 / Hive-compatible storage             │
└─────────────────────────────────────────────────┘
```
