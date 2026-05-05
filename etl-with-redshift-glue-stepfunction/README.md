# AWS Redshift ETL Pipeline with Step Functions Automation

Complete automated ETL pipeline using AWS Redshift Serverless, Glue, Step Functions, and SNS notifications.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Cost Estimates](#cost-estimates)
- [Cleanup](#cleanup)

---

## 🏗️ Architecture Overview

```
┌─────────────┐
│   S3 Bucket │ (Sample Data - Parquet Files)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              AWS Step Functions                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Job 1: Create Schema (Glue)                      │  │
│  │  - Creates Glue Data Catalog                      │  │
│  │  - Creates Redshift Spectrum external schema      │  │
│  │  - Creates Redshift internal table                │  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │ SUCCESS                          │
│                       ▼                                  │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Job 2: Load Data (Glue)                          │  │
│  │  - Loads data from S3 into Redshift               │  │
│  │  - Applies filters (year >= 2015)                 │  │
│  │  - Optimizes with DISTKEY and SORTKEY             │  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │ SUCCESS                          │
│                       ▼                                  │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Job 3: Generate Report (Glue)                    │  │
│  │  - Aggregates data by category and product        │  │
│  │  - Exports results to S3 as Parquet               │  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │ SUCCESS                          │
│                       ▼                                  │
│  ┌───────────────────────────────────────────────────┐  │
│  │  SNS Notification                                  │  │
│  │  - Sends success email with job details           │  │
│  └───────────────────────────────────────────────────┘  │
│                                                          │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Error Handler (Any Job Fails)                    │  │
│  │  - Catches errors                                  │  │
│  │  - Sends failure notification via SNS             │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│  S3 Output  │ (Aggregated Reports - Parquet)
└─────────────┘
       │
       ▼
┌─────────────┐
│ QuickSight  │ (Optional - Data Visualization)
└─────────────┘
```

---

## ✨ Features

- **Fully Automated ETL Pipeline**: 3 sequential Glue jobs orchestrated by Step Functions
- **Error Handling**: Automatic SNS notifications on success/failure
- **Serverless Architecture**: Uses Redshift Serverless, Glue Python Shell, Step Functions
- **Private VPC Access**: Redshift in private VPC with VPC endpoints (no Internet Gateway)
- **Redshift Spectrum**: Query data directly from S3 using external tables
- **Scalable**: Handles concurrent job executions
- **Cost-Effective**: Pay only for what you use (serverless components)

---

## 📦 Prerequisites

### Required Tools
- AWS CLI configured with credentials
- Python 3.9+
- boto3, pandas, pyarrow libraries

### AWS Account Requirements
- AWS Account with appropriate permissions
- Region: `us-east-1` (can be changed in configuration)

### Required AWS Permissions
- VPC, EC2, Subnets, Security Groups
- IAM Roles and Policies
- Redshift Serverless
- AWS Glue
- S3
- Secrets Manager
- Step Functions
- SNS
- CloudWatch Logs

---

## 📁 Project Structure

```
.
├── generate.py                      # Infrastructure setup script
├── upload_data.py                   # Sample data generator
├── rs_query.py                      # Redshift Data API executor
├── reviewsschema.sql                # Schema creation SQL
├── etl.sql                          # Data loading SQL
├── topreviews.sql                   # Report generation SQL
├── state_machine.json               # Step Functions definition
├── step_functions_role_policy.json  # IAM policy for Step Functions
├── step_functions_trust_policy.json # IAM trust policy
├── quicksight_manifest.json         # QuickSight S3 manifest
└── README.md                        # This file
```

---

## 🚀 Setup Instructions

### Step 1: Configure Settings

Edit `generate.py` and update these values:

```python
SCRIPTS_BUCKET   = 'redshift-scripts-data-us'    # Must be globally unique
PROCESSED_BUCKET = 'redshift-processed-data-us'  # Must be globally unique
DB_PASSWORD      = 'YourPassword123'             # Min 8 chars, 1 upper, 1 number
```

### Step 2: Install Python Dependencies

```bash
pip install boto3 pandas pyarrow
```

### Step 3: Run Infrastructure Setup

This script creates all required AWS resources:

```bash
python generate.py
```

**What it creates:**
- VPC with 2 private subnets
- Security groups
- VPC endpoints (S3, STS, Glue, Secrets Manager)
- IAM roles (GlueETLRole, RedshiftSpectrumRole)
- Redshift Serverless namespace and workgroup
- S3 buckets
- Secrets Manager secret
- Glue job
- Sample data (2000 rows in Parquet format)

**Duration:** ~5-10 minutes

**Important Notes:**
- Redshift workgroup is created with `publiclyAccessible=false` (private VPC access)
- If workgroup gets stuck in MODIFYING state, it means Internet Gateway is missing (expected for private setup)
- The script will automatically handle this and create a private workgroup

### Step 4: Upload Sample Data

The `generate.py` script automatically generates and uploads sample data. If you need to regenerate:

```bash
python upload_data.py
```

### Step 5: Create SNS Topic for Notifications

```bash
# Create SNS topic
aws sns create-topic --name redshift-etl-failures --region us-east-1

# Subscribe your email (replace with your email)
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:redshift-etl-failures \
  --protocol email \
  --notification-endpoint your-email@example.com \
  --region us-east-1
```

**Important:** Check your email and confirm the subscription!

### Step 6: Create Step Functions IAM Role

```bash
# Create IAM role
aws iam create-role \
  --role-name RedshiftETLStepFunctionsRole \
  --assume-role-policy-document file://step_functions_trust_policy.json \
  --region us-east-1

# Attach policy
aws iam put-role-policy \
  --role-name RedshiftETLStepFunctionsRole \
  --policy-name RedshiftETLStepFunctionsPolicy \
  --policy-document file://step_functions_role_policy.json \
  --region us-east-1
```

### Step 7: Update State Machine Definition

Edit `state_machine.json` and replace `YOUR_ACCOUNT_ID` with your AWS account ID in the SNS topic ARN.

### Step 8: Create Step Functions State Machine

```bash
aws stepfunctions create-state-machine \
  --name RedshiftETLPipeline \
  --definition file://state_machine.json \
  --role-arn arn:aws:iam::YOUR_ACCOUNT_ID:role/RedshiftETLStepFunctionsRole \
  --type STANDARD \
  --region us-east-1
```

### Step 9: Update Glue Job Configuration

**Important:** Set `MaxConcurrentRuns=3` to avoid concurrent execution errors:

```bash
aws glue update-job \
  --job-name myglue \
  --job-update '{
    "Role":"arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueETLRole",
    "Command":{"Name":"pythonshell","ScriptLocation":"s3://redshift-scripts-data-us/python/rs_query.py","PythonVersion":"3.9"},
    "DefaultArguments":{
      "--secret_name":"redshift/etl-credentials",
      "--region_name":"us-east-1",
      "--s3_sql_file":"s3://redshift-scripts-data-us/sql/reviewsschema.sql",
      "--additional-python-modules":"boto3>=1.26.0"
    },
    "ExecutionProperty":{"MaxConcurrentRuns":3},
    "GlueVersion":"3.0",
    "MaxCapacity":0.0625
  }' \
  --region us-east-1
```

---

## ▶️ Running the Pipeline

### Manual Execution (CLI)

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:YOUR_ACCOUNT_ID:stateMachine:RedshiftETLPipeline \
  --region us-east-1
```

### Manual Execution (AWS Console)

1. Go to AWS Step Functions console
2. Select `RedshiftETLPipeline`
3. Click "Start execution"
4. Leave input as `{}` (empty JSON)
5. Click "Start execution"

### Scheduled Execution (Optional)

Create an EventBridge rule to run the pipeline on a schedule:

```bash
# Daily at 2 AM UTC
aws events put-rule \
  --name RedshiftETLDaily \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED \
  --region us-east-1

# Add Step Functions as target
aws events put-targets \
  --rule RedshiftETLDaily \
  --targets "Id"="1","Arn"="arn:aws:states:us-east-1:YOUR_ACCOUNT_ID:stateMachine:RedshiftETLPipeline","RoleArn"="arn:aws:iam::YOUR_ACCOUNT_ID:role/RedshiftETLStepFunctionsRole" \
  --region us-east-1
```

### Expected Duration

- **Job 1 (Create Schema):** ~45 seconds
- **Job 2 (Load Data):** ~40 seconds
- **Job 3 (Generate Report):** ~25 seconds
- **Total:** ~3 minutes

### Email Notifications

You will receive email notifications:
- ✅ **Success:** "ETL Pipeline completed successfully!" with job IDs
- ❌ **Failure:** "ETL Pipeline FAILED!" with error details

---

## 🔍 Monitoring & Troubleshooting

### Check Execution Status

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:YOUR_ACCOUNT_ID:stateMachine:RedshiftETLPipeline \
  --max-results 10 \
  --region us-east-1

# Get execution details
aws stepfunctions describe-execution \
  --execution-arn <execution-arn> \
  --region us-east-1
```

### Check Glue Job Runs

```bash
aws glue get-job-runs \
  --job-name myglue \
  --max-results 10 \
  --region us-east-1
```

### Check CloudWatch Logs

```bash
# Glue job output logs
aws logs tail /aws-glue/python-jobs/output --since 1h --region us-east-1

# Glue job error logs
aws logs tail /aws-glue/python-jobs/error --since 1h --region us-east-1
```

### Check S3 Output

```bash
aws s3 ls s3://redshift-processed-data-us/output/ --recursive
```

### Common Issues

#### 1. ConcurrentRunsExceededException

**Problem:** Glue job fails with "Concurrent runs exceeded"

**Solution:** Increase `MaxConcurrentRuns` in Glue job configuration (set to 3)

#### 2. Workgroup Stuck in MODIFYING

**Problem:** Redshift workgroup stuck in MODIFYING state for hours

**Root Cause:** `publiclyAccessible=true` requires Internet Gateway, but VPC doesn't have one

**Solution:** Delete and recreate workgroup with `publiclyAccessible=false`

#### 3. Parquet Schema Mismatch

**Problem:** "Spectrum Scan Error: incompatible Parquet schema"

**Solution:** Ensure data types match Redshift schema (use int32 instead of int64)

#### 4. SNS Notifications Not Received

**Problem:** No email notifications

**Solution:**
- Check email subscription is confirmed (check spam folder)
- Verify SNS topic ARN in state_machine.json
- Check Step Functions role has SNS:Publish permission

---

## 💰 Cost Estimates

### Infrastructure Costs (Monthly - Daily Execution)

| Service | Usage | Cost |
|---------|-------|------|
| **Redshift Serverless** | 8 RPU × 3 min/day | ~$4.50/month |
| **Glue Jobs** | 0.0625 DPU × 3 min/day | ~$0.03/month |
| **Step Functions** | ~10 state transitions/day | ~$0.01/month |
| **SNS** | 30 emails/month | Free (first 1,000) |
| **S3 Storage** | ~1 GB | ~$0.02/month |
| **VPC Endpoints** | 3 endpoints × 24/7 | ~$22/month |
| **CloudWatch Logs** | Minimal | ~$0.50/month |
| **Total** | | **~$27/month** |

### One-Time Execution Cost

- Single pipeline run: **~$0.15**

---

## 🧹 Cleanup

### Delete Step Functions Resources

```bash
# Delete EventBridge rule (if created)
aws events remove-targets --rule RedshiftETLDaily --ids 1 --region us-east-1
aws events delete-rule --name RedshiftETLDaily --region us-east-1

# Delete state machine
aws stepfunctions delete-state-machine \
  --state-machine-arn arn:aws:states:us-east-1:YOUR_ACCOUNT_ID:stateMachine:RedshiftETLPipeline \
  --region us-east-1

# Delete IAM role
aws iam delete-role-policy --role-name RedshiftETLStepFunctionsRole --policy-name RedshiftETLStepFunctionsPolicy
aws iam delete-role --role-name RedshiftETLStepFunctionsRole

# Delete SNS topic
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:redshift-etl-failures --region us-east-1
```

### Delete All Resources

For complete cleanup, delete Redshift, S3, Glue, VPC, and IAM resources. See full cleanup commands in the troubleshooting section.

---

## 📚 Additional Resources

- [AWS Redshift Serverless Documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-whatis.html)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [Redshift Spectrum Documentation](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html)

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## ⚠️ Important Notes

1. **Security:** Never commit AWS credentials or secrets to version control
2. **Costs:** Monitor your AWS costs regularly, especially Redshift and VPC endpoints
3. **Region:** This setup uses `us-east-1`. Change region in all scripts if needed
4. **Bucket Names:** S3 bucket names must be globally unique - update in `generate.py`
5. **Private Access:** Redshift workgroup is private (no public access) for security
6. **MaxConcurrentRuns:** Must be set to 3 to avoid concurrent execution errors

---

**Built with ❤️ using AWS Serverless Services**
