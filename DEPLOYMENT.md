# 🚀 Deployment Guide

This guide will help you deploy and configure the AWS Glue to Iceberg Migration Tool.

## 📋 Prerequisites

### AWS Account Setup
1. **AWS Account** with appropriate permissions
2. **AWS Glue 5.0** available in your region
3. **S3 Buckets** for:
   - Source data storage
   - Iceberg table storage
   - Configuration files
   - Glue job scripts and logs

### Required AWS Services
- AWS Glue Data Catalog
- AWS Glue ETL Jobs
- Amazon S3
- Amazon Athena
- AWS IAM (for permissions)

## 🔐 IAM Setup

### 1. Create IAM Role for Streamlit App

Create an IAM role with the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabases",
                "glue:GetTables",
                "glue:GetTable",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:CreateJob",
                "glue:UpdateJob",
                "glue:GetJob"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket/*",
                "arn:aws:s3:::your-bucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
            "Resource": "*"
        }
    ]
}
```

### 2. Create IAM Role for Glue Job

Create a Glue service role with:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:*",
                "athena:*",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

## 🏗️ S3 Bucket Setup

### 1. Create Required Buckets

```bash
# Configuration bucket
aws s3 mb s3://your-config-bucket

# Iceberg warehouse bucket
aws s3 mb s3://your-iceberg-warehouse

# Glue job scripts bucket
aws s3 mb s3://your-glue-scripts
```

### 2. Upload Glue Job Script

```bash
aws s3 cp glue_job_script.py s3://your-glue-scripts/iceberg-migration-job.py
```

## 🔧 Glue Job Setup

### 1. Create Glue Job

```bash
aws glue create-job \
    --name "iceberg-migration-job" \
    --role "arn:aws:iam::YOUR_ACCOUNT:role/GlueServiceRole" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://your-glue-scripts/iceberg-migration-job.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--datalake-formats": "iceberg",
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable"
    }' \
    --glue-version "4.0" \
    --max-capacity 2 \
    --timeout 60
```

### 2. Update Job for Glue 5.0

```bash
aws glue update-job \
    --job-name "iceberg-migration-job" \
    --job-update '{
        "GlueVersion": "5.0",
        "DefaultArguments": {
            "--datalake-formats": "iceberg",
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-disable",
            "--spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "--spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "--spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
        }
    }'
```

## 🖥️ Application Deployment

### 1. Local Development

```bash
# Clone repository
git clone <repository-url>
cd awsiceburg

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
aws configure

# Run application
streamlit run app.py
```

### 2. Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Build and run:

```bash
docker build -t glue-iceberg-migration .
docker run -p 8501:8501 -e AWS_ACCESS_KEY_ID=your_key -e AWS_SECRET_ACCESS_KEY=your_secret glue-iceberg-migration
```

### 3. AWS EC2 Deployment

```bash
# Launch EC2 instance (t3.medium or larger)
# Install Docker
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user

# Deploy application
docker run -d -p 8501:8501 \
    -e AWS_ACCESS_KEY_ID=your_key \
    -e AWS_SECRET_ACCESS_KEY=your_secret \
    -e AWS_DEFAULT_REGION=your_region \
    glue-iceberg-migration
```

## ⚙️ Configuration

### 1. Environment Variables

Set the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=your_region
export CONFIG_S3_BUCKET=your-config-bucket
export ICEBERG_WAREHOUSE=s3://your-iceberg-warehouse
```

### 2. Application Configuration

Update the following in `app.py`:

```python
# Default S3 locations
DEFAULT_CONFIG_BUCKET = "your-config-bucket"
DEFAULT_WAREHOUSE_LOCATION = "s3://your-iceberg-warehouse"
DEFAULT_GLUE_JOB_NAME = "iceberg-migration-job"
```

## 🧪 Testing

### 1. Test AWS Connectivity

```python
import boto3

# Test Glue access
glue = boto3.client('glue')
databases = glue.get_databases()
print(f"Found {len(databases['DatabaseList'])} databases")

# Test S3 access
s3 = boto3.client('s3')
buckets = s3.list_buckets()
print(f"Found {len(buckets['Buckets'])} buckets")
```

### 2. Test Migration Process

1. Create a test table in Glue catalog
2. Run the Streamlit application
3. Complete the migration workflow
4. Verify the Iceberg table in Athena

## 📊 Monitoring

### 1. CloudWatch Logs

Monitor Glue job execution:

```bash
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/jobs"
```

### 2. Application Logs

For Docker deployment:

```bash
docker logs <container_id>
```

### 3. S3 Monitoring

Monitor S3 access patterns and costs in CloudWatch.

## 🔧 Troubleshooting

### Common Issues

1. **Permission Denied**
   - Verify IAM roles and policies
   - Check AWS credentials configuration

2. **Glue Job Failures**
   - Review CloudWatch logs
   - Verify Glue 5.0 availability
   - Check S3 bucket permissions

3. **Table Not Found in Athena**
   - Verify table registration in Glue catalog
   - Check Athena data source configuration
   - Ensure proper S3 permissions

### Debug Mode

Enable debug logging in the application:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📈 Performance Optimization

### 1. Glue Job Configuration

- Use appropriate DPU allocation (2-10 DPUs)
- Enable job bookmarking for incremental processing
- Configure timeout based on data size

### 2. S3 Optimization

- Use S3 Transfer Acceleration for large files
- Enable S3 Intelligent Tiering for cost optimization
- Use appropriate S3 storage classes

### 3. Application Performance

- Use appropriate EC2 instance types
- Enable application-level caching
- Monitor memory usage and scale accordingly

## 🔄 Updates and Maintenance

### 1. Application Updates

```bash
# Pull latest changes
git pull origin main

# Rebuild Docker image
docker build -t glue-iceberg-migration:latest .

# Deploy updated version
docker stop <container_id>
docker run -d -p 8501:8501 glue-iceberg-migration:latest
```

### 2. Glue Job Updates

```bash
# Update job script
aws s3 cp glue_job_script.py s3://your-glue-scripts/iceberg-migration-job.py

# Update job configuration if needed
aws glue update-job --job-name "iceberg-migration-job" --job-update '...'
```

## 📞 Support

For deployment issues:

1. Check AWS service status
2. Review CloudWatch logs
3. Verify IAM permissions
4. Test with minimal configuration
5. Contact AWS support if needed

---

**Happy Deploying! 🚀**
