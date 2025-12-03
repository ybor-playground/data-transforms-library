# YBOR Data Transforms Library - Project Overview

## Table of Contents
1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Core Components](#core-components)
5. [Manifest Configuration](#manifest-configuration)
6. [Features](#features)
7. [Usage Examples](#usage-examples)
8. [Extensibility](#extensibility)

---

## Introduction

The YBOR Data Transforms Library is a **low-code, configuration-driven data transformation framework** designed for Python-based data processing engines. It provides a declarative approach to data transformations through JSON manifest files, eliminating the need to write extensive boilerplate code for common data engineering tasks.

### Key Benefits
- **Low-Code Approach**: Define transformations in JSON manifest files instead of writing code
- **Engine Agnostic**: Abstract design allows support for multiple processing engines (currently supports Apache Spark)
- **Declarative Configuration**: Separate business logic from infrastructure code
- **Reusable Components**: Built-in helpers for common transformations
- **Extensible**: Easy to add custom transformations and new engine implementations

---

## Prerequisites

Before using the YBOR Data Transforms Library, ensure your system meets the following requirements:

### System Requirements

#### 1. Python
- **Version Required**: Python 3.11 or higher
- **Verification**: Run `python --version` or `python3 --version`

#### 2. Java Development Kit (JDK)
Apache Spark requires Java to run. The library has been tested with:
- **Recommended Version**: Java 11, 17, or 21
- **Current Setup**: OpenJDK 17 (Amazon Corretto 17.0.12)

##### Checking Java Installation

**Verify Java is installed:**
```bash
java -version
```

Expected output (example):
```
openjdk version "17.0.12" 2024-07-16 LTS
OpenJDK Runtime Environment Corretto-17.0.12.7.1 (build 17.0.12+7-LTS)
OpenJDK 64-Bit Server VM Corretto-17.0.12.7.1 (build 17.0.12+7-LTS, mixed mode, sharing)
```

**Check JAVA_HOME environment variable:**
```bash
# macOS/Linux
echo $JAVA_HOME

# Windows
echo %JAVA_HOME%
```

##### Installing Java (if not installed)

**macOS:**

Option 1 - Using Homebrew (Recommended):
```bash
# Install Amazon Corretto 17
brew install --cask corretto17

# Or install OpenJDK
brew install openjdk@17
```

Option 2 - Manual Installation:
1. Download Amazon Corretto from: https://aws.amazon.com/corretto/
2. Or download OpenJDK from: https://adoptium.net/
3. Install the downloaded package
4. Verify installation with `java -version`

**Linux (Ubuntu/Debian):**
```bash
# Update package index
sudo apt update

# Install OpenJDK 17
sudo apt install openjdk-17-jdk

# Verify installation
java -version
```

**Linux (RHEL/CentOS/Fedora):**
```bash
# Install OpenJDK 17
sudo yum install java-17-openjdk-devel

# Or using dnf
sudo dnf install java-17-openjdk-devel

# Verify installation
java -version
```

**Windows:**
1. Download and install from:
   - Amazon Corretto: https://aws.amazon.com/corretto/
   - Eclipse Temurin: https://adoptium.net/
2. Run the installer and follow the installation wizard
3. Verify installation by opening Command Prompt and running `java -version`

##### Setting JAVA_HOME

If `JAVA_HOME` is not set, configure it based on your operating system:

**macOS:**
```bash
# Find Java home directory
/usr/libexec/java_home

# Add to ~/.zshrc or ~/.bash_profile
export JAVA_HOME=$(/usr/libexec/java_home)
export PATH=$JAVA_HOME/bin:$PATH

# Apply changes
source ~/.zshrc  # or source ~/.bash_profile
```

Example path: `/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home`

**Linux:**
```bash
# Find Java installation directory
# Typical locations:
# - /usr/lib/jvm/java-17-openjdk-amd64 (Debian/Ubuntu)
# - /usr/lib/jvm/java-17-openjdk (RHEL/CentOS)

# Add to ~/.bashrc or ~/.profile
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Apply changes
source ~/.bashrc
```

**Windows:**
1. Open System Properties > Environment Variables
2. Add new System Variable:
   - Variable name: `JAVA_HOME`
   - Variable value: `C:\Program Files\Amazon Corretto\jdk17.0.12_7` (or your Java installation path)
3. Edit the `Path` system variable and add: `%JAVA_HOME%\bin`
4. Restart Command Prompt/PowerShell

**Verify JAVA_HOME is set correctly:**
```bash
# macOS/Linux
$JAVA_HOME/bin/java -version

# Windows
%JAVA_HOME%\bin\java -version
```

### Python Dependencies

The library's dependencies are managed via `pyproject.toml` and automatically installed with Poetry.

#### Core Dependencies
From `pyproject.toml`:
```toml
[tool.poetry.dependencies]
python = "^3.11"
pyspark = "^3.5.0"      # Apache Spark for Python
requests = "^2.31.0"     # HTTP library for API calls
boto3 = "^1.28.0"        # AWS SDK for Python (S3, Glue, etc.)
pyyaml = "^6.0"          # YAML parsing

[tool.poetry.group.dev.dependencies]
pytest = "^7.4.4"        # Testing framework
flake8 = "^6.1.0"        # Code linting
```

#### Installing Dependencies

**Step 1: Install Poetry (if not installed)**
```bash
# macOS/Linux
curl -sSL https://install.python-poetry.org | python3 -

# Windows (PowerShell)
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -

# Verify installation
poetry --version
```

**Step 2: Initialize the Project**
```bash
# Navigate to project directory
cd /path/to/data-transforms-library

# Install all dependencies
poetry install
```

This will:
- Create a virtual environment
- Install all dependencies from `pyproject.toml`
- Set up the project for development

**Step 3: Activate Virtual Environment**
```bash
# Activate the poetry shell
poetry shell

# Or run commands directly with poetry
poetry run python your_script.py
```

### AWS Configuration (Optional)

If using S3 data sources or AWS Glue catalog, configure AWS credentials:

#### Option 1: AWS CLI Configuration
```bash
# Install AWS CLI
pip install awscli

# Configure credentials
aws configure

# Provide:
# - AWS Access Key ID
# - AWS Secret Access Key
# - Default region (e.g., us-east-1)
# - Default output format (json)
```

#### Option 2: Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

#### Option 3: AWS Profile
Configure multiple profiles in `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY

[dev]
aws_access_key_id = DEV_ACCESS_KEY
aws_secret_access_key = DEV_SECRET_KEY
```

Reference in manifest:
```json
{
  "spark_config": {
    "input_source": "s3",
    "profile_name": "dev"
  }
}
```

### Verification Checklist

Before running transformations, verify:

- [ ] Python 3.11+ is installed: `python --version`
- [ ] Java 11/17/21 is installed: `java -version`
- [ ] JAVA_HOME is set correctly: `echo $JAVA_HOME` (macOS/Linux) or `echo %JAVA_HOME%` (Windows)
- [ ] Poetry is installed: `poetry --version`
- [ ] Dependencies are installed: `poetry install`
- [ ] Virtual environment is active: `poetry shell`
- [ ] AWS credentials configured (if using S3/Glue): `aws sts get-caller-identity`

### Troubleshooting Prerequisites

**Issue**: "JAVA_HOME is not set" error when running Spark
**Solution**: Follow the "Setting JAVA_HOME" section above for your OS

**Issue**: "java: command not found"
**Solution**: Java is not installed or not in PATH. Install Java and ensure it's added to PATH

**Issue**: Poetry install fails with dependency conflicts
**Solution**: Ensure Python 3.11+ is being used. Try `poetry env use python3.11` then `poetry install`

**Issue**: "No module named 'pyspark'" when running Python scripts
**Solution**: Activate the Poetry virtual environment with `poetry shell` or run with `poetry run python script.py`

**Issue**: AWS credentials not found
**Solution**: Configure AWS credentials using one of the three methods in the AWS Configuration section

---

## Architecture

### Design Pattern
The library follows the **Strategy Pattern** with an abstract base class that defines the contract for all processing engines.

```
┌─────────────────────────────────────────────────────────────┐
│                         Driver                              │
│  (Orchestrates the transformation pipeline)                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ uses
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                     BaseEngine (ABC)                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ • load_data()                                         │  │
│  │ • apply_transformations(df)                           │  │
│  │ • save_output(df)                                     │  │
│  │ • perform_data_quality_checks(df)                     │  │
│  └───────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ implements
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    SparkEngine                               │
│  (Concrete implementation for Apache Spark)                 │
└─────────────────────────────────────────────────────────────┘
```

### Core Workflow

1. **Load Data** (`load_data()`)
   - Reads input datasets defined in the manifest
   - Supports files (CSV, Parquet, JSON, etc.) and tables
   - Returns dictionary of DataFrames

2. **Apply Transformations** (`apply_transformations(df)`)
   - Applies cleaning, renaming, type casting
   - Executes custom conversions and functions
   - Performs aggregations and derived column generation

3. **Data Quality Checks** (`perform_data_quality_checks(df)`)
   - Validates data against defined rules
   - Performs null checks, range checks
   - Reports quality metrics

4. **Save Output** (`save_output(df)`)
   - Writes to files or catalog tables
   - Supports partitioning and deduplication
   - Handles merge operations for incremental loads

---

## Core Components

### 1. Engine Package (`src/data_transforms_library/ybor/engine/`)

#### `base.py`
Abstract base class defining the interface for all processing engines.

```python
class BaseEngine(ABC):
    @abstractmethod
    def load_data(self):
        """Load datasets defined in manifest"""
        pass

    @abstractmethod
    def apply_transformations(self, df):
        """Apply transformations from manifest"""
        pass

    @abstractmethod
    def save_output(self, df):
        """Save transformed data to target"""
        pass

    @abstractmethod
    def perform_data_quality_checks(self, df):
        """Run data quality validations"""
        pass
```

#### `spark_engine.py`
Concrete implementation for Apache Spark with the following capabilities:
- **Spark Session Management**: Configurable with custom settings
- **S3 Integration**: Built-in AWS credentials handling
- **Table Operations**: Create, replace, merge operations
- **Partition Management**: Partition-level overwrite support
- **Deduplication**: Primary key-based deduplication with sorting
- **Catalog Integration**: AWS Glue and Iceberg catalog support

### 2. Utils Package (`src/data_transforms_library/ybor/utils/`)

#### `data_loader.py`
Handles loading data from various sources:
- **File Sources**: CSV, Parquet, JSON, ORC with configurable options
- **Table Sources**: Catalog tables (Hive, Glue, Iceberg)
- Returns dictionary mapping dataset names to DataFrames

#### `data_transformer.py`
Core transformation engine with support for:
- **Cleaning**: Column renaming, casting, conversions
- **Aggregations**: groupBy, cube, rollup operations
- **Derived Columns**: split, concat, hash operations
- **Static Columns**: Add constant values or PySpark functions
- **Column Dropping**: Remove unwanted columns

#### `data_quality.py`
Data quality validation framework:
- **Null Checks**: Identify columns with null values
- **Range Checks**: Validate numeric ranges
- **Statistics Publishing**: Generate data profiling reports

#### `transformation_util.py`
Utility functions for data transformations:
- **Column Cleaning**: Main `clean_df()` method orchestrates cleaning
- **Custom Functions**: MD5 encryption, random value generation
- **Configuration Parsing**: Handles complex manifest configurations
- **Metadata Management**: Optional metadata extraction

#### `config_util.py`
Configuration management utilities:
- AWS credentials loading
- Environment variable handling
- Configuration file parsing

### 3. Helpers Package (`src/data_transforms_library/ybor/helpers/`)

#### `Conversions.py`
Pre-built conversion functions for common data transformations:

| Function | Description | Example |
|----------|-------------|---------|
| `char_to_boolean` | Convert 'Y'/'N' to True/False | 'Y' → True |
| `int_to_boolean` | Convert 1/0 to True/False | 1 → True |
| `lower` | Convert to lowercase with null handling | 'HELLO' → 'hello' |
| `normalize_gender` | Standardize gender values | 'Female' → 'F' |
| `nullify` | Convert common null representations to None | 'NA' → None |
| `clean_date_range` | Validate dates within acceptable range | Filters 1970-present |
| `nullify_future_dates` | Remove dates in the future | Future dates → None |
| `round_decimal` | Round decimal values | 3.14159 → 3.1416 |
| `date_from_string` | Parse and format date strings | '01/15/2024' → '2024-01-15' |

#### `Casts.py`
Type casting utilities for Spark data types:

| Function | Target Type | Use Case |
|----------|-------------|----------|
| `to_date` | DateType | Date columns |
| `to_decimal` | DecimalType | Precise numeric values |
| `to_integer` | IntegerType | Whole numbers |
| `to_long` | LongType | Large integers |
| `to_string` | StringType | Text data |
| `to_double` | DoubleType | Floating-point |
| `to_float` | FloatType | Floating-point (smaller) |

### 4. Driver Package (`src/data_transforms_library/ybor/driver/`)

#### `driver.py`
Orchestration layer that:
- Instantiates the appropriate engine based on configuration
- Executes the transformation pipeline in sequence
- Manages engine lifecycle (initialization and shutdown)

**Usage Flow:**
```python
driver = Driver(config)
driver.run()  # Executes: load → transform → quality checks → save
```

---

## Manifest Configuration

The manifest is a JSON file that declaratively defines the entire data transformation pipeline.

### Configuration Structure

```json
{
  "processing_engine": "spark",
  "spark_config": { ... },
  "input_datasets": [ ... ],
  "transformations": { ... },
  "output_dataset": { ... },
  "data_quality_checks": { ... }
}
```

### 1. Spark Configuration

```json
{
  "spark_config": {
    "appName": "MyTransformationJob",
    "master": "local[*]",
    "input_source": "s3",
    "profile_name": "default",
    "config": {
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.legacy.timeParserPolicy": "LEGACY"
    }
  }
}
```

**Parameters:**
- `appName`: Name of the Spark application
- `master`: Spark master URL (optional for cloud environments)
- `input_source`: Source type ('s3' enables AWS credential loading)
- `profile_name`: AWS profile for credentials
- `config`: Dictionary of Spark configuration properties

### 2. Input Datasets

#### File-based Input
```json
{
  "input_datasets": [{
    "name": "sales_data",
    "type": "file",
    "format": "csv",
    "path": "s3://bucket/path/to/data.csv",
    "options": {
      "header": "true",
      "inferSchema": "true",
      "quote": "\"",
      "escape": "\""
    }
  }]
}
```

#### Table-based Input
```json
{
  "input_datasets": [{
    "name": "customer_table",
    "type": "table",
    "table": "catalog.database.customers"
  }]
}
```

### 3. Transformations

#### Standard Cleaning (Column Operations)

```json
{
  "transformations": {
    "cleaning": {
      "standard": {
        "source_column": {
          "output": "target_column",
          "cast": "string",
          "conversion": {
            "method": "date_from_string",
            "args": {
              "source_format": "MM/dd/yyyy",
              "target_format": "yyyy-MM-dd"
            }
          },
          "function": "transform_util.md5_encrypt"
        }
      }
    }
  }
}
```

**Standard Transformation Options:**
- `output`: New column name (required)
- `cast`: Target data type (optional) - see Casts helper
- `conversion`: Built-in conversion method (optional) - see Conversions helper
- `function`: Custom function reference (optional)
- `index`: For array columns, extract specific element (optional)
- `default`: Default value for missing columns (optional)

#### Static Columns

```json
{
  "cleaning": {
    "static": {
      "environment": "production",
      "load_date": "F.current_date()"
    }
  }
}
```

Add constant columns or PySpark function expressions (prefix with `F.`).

#### Derived Columns

##### Split Operation
```json
{
  "derived_columns": {
    "first_name": {
      "operation": "split",
      "params": {
        "input_column": "full_name",
        "delimiter": " ",
        "index": 0
      }
    }
  }
}
```

##### Concatenation
```json
{
  "derived_columns": {
    "full_address": {
      "operation": "concat",
      "params": {
        "input_columns": ["street", "city", "state"],
        "separator": ", "
      }
    }
  }
}
```

##### Hash (Composite Key)
```json
{
  "derived_columns": {
    "composite_key": {
      "operation": "concat_hash",
      "params": {
        "input_columns": ["customer_id", "order_id"],
        "separator": "-",
        "hash_type": "md5"
      }
    }
  }
}
```

**Supported hash types:** `md5`, `sha1`, `sha256`, `xxhash64`

#### Aggregations

```json
{
  "transformations": {
    "aggregation": {
      "type": "groupby",
      "group_by": ["customer_id", "product_category"],
      "metrics": [
        {"column": "sales_amount", "function": "sum"},
        {"column": "order_id", "function": "count"},
        {"column": "quantity", "function": "distinct_count"}
      ]
    }
  }
}
```

**Aggregation Types:**
- `groupby`: Standard GROUP BY aggregation
- `cube`: CUBE operation (all combinations)
- `rollup`: ROLLUP operation (hierarchical subtotals)

**Metric Functions:**
- `sum`: Sum of values
- `count`: Count of records
- `count_non_null`: Count excluding nulls
- `percent_non_null`: Percentage of non-null values
- `percent_null`: Percentage of null values
- `distinct_count`: Count of unique values

#### Custom Configuration

```json
{
  "cleaning": {
    "custom": {
      "generate_etl_cols": "true",
      "add_meta": "false",
      "project_all": "false"
    }
  }
}
```

**Custom Options:**
- `generate_etl_cols`: Add `etl_processed_date` timestamp column
- `add_meta`: Extract metadata into `source_meta` struct column
- `project_all`: Include all original columns plus transformations

#### Drop Columns

```json
{
  "cleaning": {
    "drop_columns": ["temp_column", "unused_field"]
  }
}
```

### 4. Output Configuration

#### File Output
```json
{
  "output_dataset": {
    "type": "file",
    "format": "parquet",
    "path": "s3://bucket/output/path/",
    "partition_by": ["year", "month"],
    "options": {
      "mode": "overwrite",
      "compression": "snappy"
    }
  }
}
```

#### Table Output
```json
{
  "output_dataset": {
    "type": "table",
    "catalog": "glue_catalog",
    "database": "analytics",
    "table": "customer_metrics",
    "partition_by": ["load_date"],
    "primary_key": ["customer_id"],
    "sort_by": ["updated_timestamp"],
    "options": {
      "mode": "append"
    }
  }
}
```

**Table Output Features:**
- **Deduplication**: When `primary_key` and `sort_by` are specified, duplicates are removed keeping the latest record (highest sort_by value)
- **Merge Operations**: In `append` mode with `primary_key`, performs MERGE INTO (update existing, insert new)
- **Partition Overwrite**: In `overwrite` mode with partitions, only overwrites affected partitions
- **Table Creation**: Automatically creates table if it doesn't exist

**Mode Options:**
- `overwrite`: Replace existing data
- `append`: Add new data (use with `primary_key` for upsert)

### 5. Data Quality Checks

```json
{
  "data_quality_checks": {
    "checks": [
      {
        "check_type": "null_check",
        "columns": ["customer_id", "order_date", "amount"]
      },
      {
        "check_type": "range_check",
        "column": "age",
        "min": 0,
        "max": 120
      }
    ]
  }
}
```

**Check Types:**
- `null_check`: Identifies and reports null values in specified columns
- `range_check`: Validates numeric columns are within specified range

---

## Features

### 1. Low-Code Data Transformations

**Traditional Approach (Code-Heavy):**
```python
# Requires writing extensive PySpark code
df = spark.read.csv("input.csv", header=True)
df = df.withColumnRenamed("Order ID", "order_id")
df = df.withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
df = df.withColumn("order_total", col("Sales").cast("float"))
# ... many more lines
```

**YBOR Approach (Configuration-Driven):**
```json
{
  "input_datasets": [{"name": "orders", "type": "file", "format": "csv", "path": "input.csv"}],
  "transformations": {
    "cleaning": {
      "standard": {
        "Order ID": {"output": "order_id"},
        "Order Date": {"output": "order_date", "conversion": {"method": "date_from_string", "args": {"source_format": "MM/dd/yyyy"}}},
        "Sales": {"output": "order_total", "cast": "float"}
      }
    }
  }
}
```

### 2. Multi-Engine Support

The abstract base class allows easy addition of new processing engines:

```python
# Future implementations could include:
class DaskEngine(BaseEngine):
    # Implementation for Dask
    pass

class PandasEngine(BaseEngine):
    # Implementation for Pandas
    pass

class PolarsEngine(BaseEngine):
    # Implementation for Polars
    pass
```

### 3. Built-in Data Quality Framework

Automatically validate data quality without additional code:
- Null value detection and reporting
- Range validation for numeric columns
- Statistical profiling
- Extensible for custom checks

### 4. Advanced Table Management

#### Deduplication Strategy
When primary_key and sort_by are configured:
```python
# Automatically generates window function logic
window = Window.partitionBy(*primary_key).orderBy(*[F.desc(col) for col in sort_by])
df = df.withColumn("row_num", F.row_number().over(window))
  .filter(F.col("row_num") == 1)
  .drop("row_num")
```

#### Merge Operations (Upsert)
Automatically generates MERGE statements:
```sql
MERGE INTO target_table as target
USING source_data as source
ON (target.customer_id = source.customer_id)
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 5. AWS Integration

Built-in support for:
- S3 data sources and targets
- AWS Glue catalog
- Iceberg tables
- Credential management via AWS profiles

### 6. Flexible Column Transformations

Support for complex transformation chains:
```json
{
  "product_id": {
    "output": "encrypted_product_id",
    "cast": "string",
    "conversion": {"method": "lower"},
    "function": "transform_util.md5_encrypt"
  }
}
```

This applies: source → cast to string → lowercase → MD5 hash → output

---

## Usage Examples

### Example 1: Simple CSV Transformation

**Manifest:** `examples/orders.json`

```json
{
  "input_datasets": [{
    "name": "superstore",
    "type": "file",
    "format": "csv",
    "path": "../resources/superstore.csv",
    "options": {"header": "true", "inferSchema": "true"}
  }],
  "transformations": {
    "cleaning": {
      "standard": {
        "Row ID": {"output": "transaction_id"},
        "Order ID": {"output": "order_id"},
        "Order Date": {
          "output": "order_date",
          "conversion": {
            "method": "date_from_string",
            "args": {"source_format": "MM/dd/yyyy", "target_format": "yyyy-MM-dd"}
          }
        },
        "Sales": {"output": "order_total"},
        "Quantity": {"output": "order_quantity"}
      },
      "custom": {"generate_etl_cols": "true"}
    }
  },
  "output_dataset": {
    "type": "file",
    "format": "csv",
    "path": "../examples/out/orders",
    "options": {"mode": "overwrite", "header": "true"}
  },
  "spark_config": {
    "appName": "OrdersTransformation",
    "config": {"spark.sql.legacy.timeParserPolicy": "LEGACY"}
  }
}
```

**Python Code:**
```python
import json
from data_transforms_library.ybor.driver.driver import Driver

with open('examples/orders.json', 'r') as f:
    config = json.load(f)

driver = Driver(config)
driver.run()
```

### Example 2: Advanced Transformations with Encryption

**Manifest:** `examples/sales.json`

```json
{
  "input_datasets": [{
    "name": "superstore",
    "type": "file",
    "format": "csv",
    "path": "../resources/sales.csv",
    "options": {"header": "true", "inferSchema": "true"}
  }],
  "transformations": {
    "cleaning": {
      "static": {
        "source_name": "datalib",
        "venture_name": "sales"
      },
      "standard": {
        "product_id": {
          "output": "EncryptedProduct_ID",
          "function": "transform_util.md5_encrypt"
        },
        "date": {
          "output": "TransactionDate",
          "conversion": {
            "method": "date_from_string",
            "args": {"target_format": "yyyy/MM/dd"}
          }
        },
        "sales": {"output": "SalesAmount", "cast": "float"},
        "stock": {"output": "NoOfItems", "cast": "integer"}
      },
      "custom": {"generate_etl_cols": "true"}
    }
  },
  "output_dataset": {
    "type": "file",
    "format": "csv",
    "path": "../examples/out/sales",
    "options": {"mode": "overwrite", "header": "true"}
  },
  "spark_config": {
    "appName": "SalesTransformation",
    "config": {"spark.sql.legacy.timeParserPolicy": "LEGACY"}
  }
}
```

### Example 3: Table-to-Table with Deduplication

```json
{
  "processing_engine": "spark",
  "input_datasets": [{
    "name": "raw_customers",
    "type": "table",
    "table": "bronze.raw_customers"
  }],
  "transformations": {
    "cleaning": {
      "standard": {
        "customer_id": {"output": "customer_id"},
        "customer_name": {"output": "name", "conversion": {"method": "nullify"}},
        "email": {"output": "email", "cast": "string"},
        "created_at": {"output": "created_timestamp"}
      },
      "custom": {"generate_etl_cols": "true"}
    }
  },
  "output_dataset": {
    "type": "table",
    "catalog": "glue_catalog",
    "database": "silver",
    "table": "clean_customers",
    "primary_key": ["customer_id"],
    "sort_by": ["created_timestamp"],
    "partition_by": ["load_date"],
    "options": {"mode": "append"}
  },
  "data_quality_checks": {
    "checks": [
      {"check_type": "null_check", "columns": ["customer_id", "email"]}
    ]
  },
  "spark_config": {
    "appName": "CustomerCleaning",
    "config": {}
  }
}
```

### Example 4: Aggregation Pipeline

```json
{
  "input_datasets": [{
    "name": "transactions",
    "type": "table",
    "table": "silver.transactions"
  }],
  "transformations": {
    "aggregation": {
      "type": "groupby",
      "group_by": ["customer_id", "product_category"],
      "metrics": [
        {"column": "amount", "function": "sum"},
        {"column": "transaction_id", "function": "count"},
        {"column": "customer_id", "function": "distinct_count"}
      ]
    }
  },
  "output_dataset": {
    "type": "table",
    "catalog": "glue_catalog",
    "database": "gold",
    "table": "customer_metrics",
    "options": {"mode": "overwrite"}
  }
}
```

---

## Extensibility

### Adding a New Processing Engine

1. **Create Engine Class:**
```python
from data_transforms_library.ybor.engine.base import BaseEngine

class MyCustomEngine(BaseEngine):
    def __init__(self, config):
        super().__init__(config)
        # Initialize your engine

    def load_data(self):
        # Implement data loading logic
        pass

    def apply_transformations(self, df):
        # Implement transformation logic
        pass

    def save_output(self, df):
        # Implement save logic
        pass

    def perform_data_quality_checks(self, df):
        # Implement quality checks
        pass
```

2. **Register in Driver:**
```python
# In driver.py
def _load_engine(self, engine_name):
    if engine_name == "spark":
        return SparkEngine(self.config)
    elif engine_name == "mycustom":
        return MyCustomEngine(self.config)
    else:
        raise ValueError(f"Unsupported engine: {engine_name}")
```

### Adding Custom Conversions

Add to `helpers/Conversions.py`:
```python
@staticmethod
def custom_conversion(input_column, **kwargs):
    # Your custom logic
    return transformed_column
```

Use in manifest:
```json
{
  "column_name": {
    "output": "new_name",
    "conversion": {
      "method": "custom_conversion",
      "args": {"param1": "value1"}
    }
  }
}
```

### Adding Custom Cast Types

Add to `helpers/Casts.py`:
```python
@staticmethod
def to_custom_type(input_column):
    return input_column.cast(CustomType())
```

### Adding Custom Quality Checks

Extend `utils/data_quality.py`:
```python
def _custom_check(self, data, **params):
    # Implement your check logic
    pass
```

Register in `perform_checks()`:
```python
elif check["check_type"] == "custom_check":
    self._custom_check(self.data, **check)
```

---

## Best Practices

### 1. Manifest Organization
- Keep manifests in version control
- Use descriptive names: `job_name_env.json`
- Document complex transformations with comments (if JSON5 is used)

### 2. Performance Optimization
- Apply filters early in transformation pipeline
- Use appropriate partition strategies
- Leverage predicate pushdown with table sources
- Use appropriate file formats (Parquet for analytics)

### 3. Data Quality
- Always define null checks for critical columns
- Add range checks for numeric business metrics
- Validate referential integrity for join keys

### 4. Output Management
- Use partitioning for large datasets
- Configure appropriate deduplication strategies
- Choose merge mode for incremental loads
- Test partition overwrite behavior before production

### 5. Error Handling
- Review transformation logs for warnings
- Test manifests with sample data first
- Validate data types match expected formats
- Use schema validation for input sources

---

## File Reference Guide

### Core Files

| File Path | Purpose | Key Classes/Functions |
|-----------|---------|----------------------|
| `src/data_transforms_library/ybor/engine/base.py` | Abstract engine interface | `BaseEngine` |
| `src/data_transforms_library/ybor/engine/spark_engine.py` | Spark implementation | `SparkEngine` |
| `src/data_transforms_library/ybor/driver/driver.py` | Pipeline orchestration | `Driver` |
| `src/data_transforms_library/ybor/utils/data_loader.py` | Data ingestion | `DataLoader` |
| `src/data_transforms_library/ybor/utils/data_transformer.py` | Transformation logic | `DataTransformer` |
| `src/data_transforms_library/ybor/utils/transformation_util.py` | Transformation utilities | `TransformationsUtil` |
| `src/data_transforms_library/ybor/utils/data_quality.py` | Quality validation | `DataQualityChecker` |
| `src/data_transforms_library/ybor/helpers/Conversions.py` | Conversion functions | `Conversions` |
| `src/data_transforms_library/ybor/helpers/Casts.py` | Type casting | `Casts` |

### Example Files

| File Path | Description |
|-----------|-------------|
| `src/data_transforms_library/ybor/examples/orders.json` | CSV transformation with date parsing |
| `src/data_transforms_library/ybor/examples/sales.json` | Transformation with encryption and static columns |
| `src/data_transforms_library/ybor/examples/example.py` | Python usage example with Spark SQL |

---

## Troubleshooting

### Common Issues

**Issue:** "Column not found" error
**Solution:** Verify input column names match source data exactly (case-sensitive)

**Issue:** Date parsing failures
**Solution:** Ensure `source_format` and `target_format` match your data format

**Issue:** S3 access denied
**Solution:** Check AWS credentials are properly configured and profile_name is correct

**Issue:** Table not created
**Solution:** Ensure catalog and database exist and proper permissions are granted

**Issue:** Deduplication not working
**Solution:** Verify both `primary_key` and `sort_by` are specified in output configuration

---

## Summary

The YBOR Data Transforms Library provides a powerful, flexible framework for building data transformation pipelines with minimal code. By leveraging JSON manifests and a pluggable engine architecture, it enables data engineers to focus on business logic rather than boilerplate code, while maintaining the flexibility to extend and customize as needed.

### Key Takeaways
- **Declarative approach** reduces development time
- **Engine-agnostic design** supports multiple processing frameworks
- **Built-in features** handle common patterns (dedup, merge, partitioning)
- **Extensible architecture** allows custom enhancements
- **Production-ready** with quality checks and error handling
