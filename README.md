# ❄️ AWS Glue to Iceberg Migration Tool

A comprehensive Streamlit application for migrating AWS Glue catalog tables to Apache Iceberg tables using AWS Glue 5.0. This tool provides an intuitive interface for managing the complete migration process from source table selection to final Iceberg table creation.

## 🌟 Features

- **Complete Migration Workflow**: Database/table selection, metadata analysis, data type mapping, target configuration, and execution
- **Beautiful UI**: Modern gradient design with responsive layout and progress tracking
- **Advanced Configuration**: Performance settings, compaction options, retention policies
- **Athena Integration**: Seamless querying of migrated Iceberg tables

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- AWS CLI configured with appropriate permissions
- AWS Glue 5.0 environment
- Access to S3 buckets

### Installation

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure AWS credentials**
   ```bash
   aws configure
   ```

3. **Run the application**
   ```bash
   streamlit run app.py
   ```

## 📋 Migration Process

1. **Select Source**: Choose database and table from Glue catalog
2. **Review Metadata**: Analyze table structure, columns, and partitions
3. **Map Data Types**: Configure Glue to Iceberg data type mappings
4. **Set Target**: Configure target database, table, and S3 location
5. **Configure Iceberg**: Set performance and optimization parameters
6. **Execute**: Generate config and run Glue job to create Iceberg table

## 🔧 Configuration Options

### Data Type Mappings
- Intelligent default mappings between Glue and Iceberg types
- Customizable mappings for specific requirements
- Support for all major data types (string, int, float, boolean, date, etc.)

### Performance Settings
- **File Formats**: Parquet, ORC, Avro
- **Compression**: Snappy, Gzip, LZ4, Zstd
- **File Sizes**: Configurable target and write file sizes
- **Compaction**: Automatic compaction with retention policies

## 🔐 Required AWS Permissions

- Glue: GetDatabases, GetTables, GetTable, StartJobRun, GetJobRun
- S3: GetObject, PutObject, DeleteObject, ListBucket
- Athena: StartQueryExecution, GetQueryExecution, GetQueryResults

## 🏗️ AWS Glue Job Configuration

The tool generates configuration files with:
- `--datalake-formats=iceberg`
- Spark extensions for Iceberg support
- Catalog and warehouse configurations
- Performance optimization settings

## 🔍 Athena Integration

After migration, tables are automatically available in Athena with:
- ACID transactions
- Time travel capabilities
- Schema evolution
- Optimized analytical performance

## 🛠️ Troubleshooting

- Verify AWS credentials and permissions
- Check Glue job logs in AWS console
- Ensure S3 bucket access and policies
- Validate Glue 5.0 availability in your region

## 📊 Performance Tips

- Use 128-512MB target file sizes
- Enable automatic compaction
- Choose appropriate compression (Snappy recommended)
- Consider partitioning strategy for large tables

---

**Happy Migrating! 🚀❄️**
