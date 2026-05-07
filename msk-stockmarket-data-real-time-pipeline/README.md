# FMP Pipeline with Airflow and AWS MSK

## Overview

A production-grade real-time streaming data pipeline that ingests financial market data, processes it through Amazon MSK (Managed Streaming for Kafka), stores it in S3, and loads it into Snowflake for analytics. This project demonstrates enterprise-level data engineering with AWS managed services.

## Project Objective

Build a scalable real-time data pipeline that:
- Streams stock market data using Apache Kafka on AWS MSK
- Implements IAM-based authentication for secure access
- Automatically flushes data to S3 using MSK Connect
- Ingests data into Snowflake using Snowpipe
- Provides queryable analytics-ready data

## Architecture

```
┌─────────────────┐
│ Python Producer │ (Stock market data)
└────────┬────────┘
         │ Kafka Protocol + IAM Auth
         ▼
┌─────────────────┐
│   Amazon MSK    │ (Managed Kafka Cluster)
│   Multi-AZ      │
└────────┬────────┘
         │ MSK Connect (S3 Sink Connector)
         ▼
┌─────────────────┐
│   Amazon S3     │ (Data Lake - JSON files)
└────────┬────────┘
         │ S3 Event → SQS
         ▼
┌─────────────────┐
│   Snowpipe      │ (Auto-ingestion)
└────────┬────────┘
         ▼
┌─────────────────┐
│   Snowflake     │ (Data Warehouse)
└─────────────────┘
```

## Technology Stack

- **Amazon MSK**: Managed Kafka service
- **AWS EC2**: Kafka client instances
- **AWS VPC**: Network isolation
- **MSK Connect**: Kafka Connect managed service
- **Confluent S3 Sink Connector**: Data export to S3
- **Amazon S3**: Data lake storage
- **Snowflake**: Cloud data warehouse
- **Snowpipe**: Automated data ingestion
- **Python**: Producer application
- **IAM**: Authentication and authorization

## Key Features

### 1. Real-Time Streaming
- Continuous data ingestion from producers
- Low-latency message processing
- Scalable Kafka cluster with multi-AZ deployment

### 2. Secure Architecture
- IAM-based authentication for MSK
- VPC isolation with public/private subnets
- Security groups for network access control
- Encrypted data in transit and at rest

### 3. Automated Data Flow
- MSK Connect automatically exports to S3
- Snowpipe detects and loads new files
- No manual intervention required
- End-to-end automation

### 4. Production-Ready
- High availability across availability zones
- Monitoring and logging enabled
- Cost optimization strategies
- Comprehensive troubleshooting guide

## Project Structure

```
fmp-pipeline-airflow/
├── README.md                    # This file
├── kp-scd-warehouse-us.pem      # EC2 key pair (private)
└── msk-stockmarket-data-real-time-pipeline/
    ├── README.md                # Detailed implementation guide
    ├── kafka-producer.py        # Stock data producer
    ├── kafka-consumer.py        # Consumer for testing
    ├── requirements.txt         # Python dependencies
    ├── .env.example             # Environment variables template
    ├── architecture-diagrams/   # Architecture visuals
    ├── config/                  # Kafka configurations
    ├── iam-policies/            # IAM policy documents
    └── sql/                     # Snowflake SQL scripts
```

## Quick Start

### Prerequisites
- AWS Account with admin access
- Snowflake account (free trial available)
- AWS CLI configured
- Python 3.9+
- SSH client

### Estimated Setup Time
**~2.5 hours** (includes waiting for MSK cluster creation)

### High-Level Steps

1. **VPC & Networking** (~15 min)
   - Create VPC with public/private subnets
   - Configure route tables and internet gateway

2. **Security Groups** (~10 min)
   - Set up security groups for EC2 and MSK

3. **EC2 Instances** (~10 min)
   - Launch public and private EC2 instances

4. **Amazon MSK Cluster** (~25 min)
   - Create MSK cluster with IAM authentication
   - Wait for cluster to become active

5. **Kafka Setup** (~15 min)
   - Create Kafka topic
   - Test producer and consumer

6. **S3 & IAM** (~15 min)
   - Create S3 bucket
   - Configure IAM roles and policies

7. **MSK Connect** (~30 min)
   - Deploy S3 Sink Connector
   - Verify data flow to S3

8. **Snowflake Setup** (~20 min)
   - Create database and tables
   - Configure Snowpipe
   - Set up S3 integration

9. **Validation** (~10 min)
   - End-to-end testing
   - Query data in Snowflake

## Data Flow

1. **Producer** generates stock market data every second
2. **MSK** receives and stores messages in Kafka topics
3. **MSK Connect** batches messages (100 records or 60 seconds)
4. **S3** stores JSON files in organized structure
5. **Snowpipe** automatically ingests new files
6. **Snowflake** makes data available for querying

## Key Components

### Amazon MSK Configuration
- **Cluster Type**: Provisioned
- **Broker Type**: kafka.t3.small
- **Brokers**: 2 (Multi-AZ)
- **Authentication**: IAM
- **Encryption**: TLS in-transit

### MSK Connect Configuration
- **Connector**: Confluent S3 Sink v12.1.4
- **Flush Size**: 100 messages
- **Flush Interval**: 60 seconds
- **Format**: JSON
- **Partitioner**: Time-based

### Snowflake Configuration
- **Database**: STOCK_MARKET_DB
- **Schema**: PUBLIC
- **Table**: STOCK_DATA
- **Ingestion**: Snowpipe (auto-ingest)
- **Format**: JSON

## Monitoring & Troubleshooting

The detailed README includes:
- Comprehensive troubleshooting decision tree
- Common issues and solutions
- Monitoring best practices
- Cost optimization tips
- Cleanup procedures

## Cost Considerations

**Estimated Monthly Cost** (if running 24/7):
- MSK Cluster: ~$150
- EC2 Instances: ~$30
- S3 Storage: ~$5
- Data Transfer: ~$10
- **Total**: ~$195/month

**Cost Optimization**:
- Stop EC2 instances when not in use
- Delete MSK cluster after testing
- Use t3.small brokers for development
- Clean up S3 data regularly

## Detailed Documentation

For complete step-by-step implementation guide with screenshots and troubleshooting:
**[msk-stockmarket-data-real-time-pipeline/README.md](./msk-stockmarket-data-real-time-pipeline/README.md)**

## Key Learnings

- Amazon MSK cluster setup and management
- Kafka producer/consumer development with IAM auth
- MSK Connect and Kafka Connect configuration
- S3 event-driven architectures
- Snowflake Snowpipe automation
- AWS VPC networking and security
- Real-time data pipeline design patterns

## Cleanup

```bash
# Delete in reverse order to avoid dependencies
1. Delete Snowpipe
2. Delete MSK Connect connector
3. Delete MSK cluster
4. Terminate EC2 instances
5. Delete S3 bucket contents
6. Delete VPC and associated resources
```

## Use Cases

- Real-time stock market analytics
- Financial data streaming
- Event-driven architectures
- Kafka on AWS learning
- Snowflake integration patterns

## References

- [Amazon MSK Documentation](https://docs.aws.amazon.com/msk/)
- [Confluent S3 Sink Connector](https://docs.confluent.io/kafka-connect-s3-sink/)
- [Snowflake Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe.html)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
