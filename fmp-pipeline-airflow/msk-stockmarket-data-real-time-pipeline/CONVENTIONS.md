# Naming Conventions & Resource Reference

This document lists every AWS/Snowflake resource name used in the project so you can match the README exactly.

---

## AWS Region

All resources in: **`us-east-1`** (N. Virginia)

---

## VPC & Networking

| Resource | Name |
|---|---|
| VPC | `kafka-snowflake-vpc` — CIDR `10.0.0.0/16` |
| Public Subnet AZ-a | `public-subnet-1a` — `10.0.1.0/24` |
| Public Subnet AZ-b | `public-subnet-1b` — `10.0.2.0/24` |
| Private Subnet AZ-a | `private-subnet-1a` — `10.0.3.0/24` |
| Private Subnet AZ-b | `private-subnet-1b` — `10.0.4.0/24` |
| Internet Gateway | `kafka-igw` |
| NAT Gateway | `kafka-nat-gw` (in `public-subnet-1a`) |
| Route Table (public) | `public-rt` |
| Route Table (private) | `private-rt` |
| S3 VPC Endpoint | `s3-vpc-endpoint` |

---

## Security Groups

| Resource | SG Name | Note |
|---|---|---|
| pub-ec2 | `sg-pub-ec2` | |
| prt-ec2 | `sg-prt-ec2` | |
| MSK Cluster | `sg-msk` | |
| MSK Connect | `mskconnect-sg` | NOT `sg-mskconnect` |

---

## EC2 Instances

| Instance | Name | Subnet | IP Type |
|---|---|---|---|
| Bastion host | `pub-ec2` | `public-subnet-1a` | Public |
| Kafka client | `prt-ec2` | `private-subnet-1a` | Private only |
| Key pair | `kafka-key` | — | `.pem` (Mac/Linux) or `.ppk` (Windows) |

---

## MSK

| Resource | Value |
|---|---|
| Cluster name | `kafka-msk-cluster` |
| Kafka version | `3.5.1` |
| Broker type | `kafka.t3.small` |
| Topic name | `stock-market-data` |
| Topic partitions | `3` |
| Replication factor | `2` |
| Auth port | `9098` (IAM / SASL_SSL) |

---

## IAM Roles

| Role | Purpose |
|---|---|
| `ec2-msk-role` | Attached to both EC2 instances |
| `msk-connect-role` | Used by MSK Connect workers |
| `snowflake-s3-role` | Used by Snowflake storage integration |

---

## S3

| Resource | Value |
|---|---|
| Bucket name | `kafka-msk-snowflake-bucket` |
| Region | `us-east-1` |
| Topics folder | `topics/stock-market-data/` |
| Plugins folder | `plugins/` |

---

## MSK Connect

| Resource | Value |
|---|---|
| Plugin name | `confluent-s3-sink-plugin` |
| Connector name | `msk-s3-sink-connector` |
| Connector class | `io.confluent.connect.s3.S3SinkConnector` |
| Plugin version used | `confluentinc-kafka-connect-s3-12.1.4.zip` |
| CloudWatch log group | `/msk-connect/msk-s3-sink-connector` |

---

## Snowflake

| Resource | Value |
|---|---|
| Database | `KAFKA_DB` |
| Schema | `KAFKA_SCHEMA` |
| Table | `STOCK_MARKET_DATA` |
| Storage integration | `s3_kafka_integration` |
| Stage | `kafka_s3_stage` |
| Pipe | `kafka_snowpipe` |

---

## Python Files (deploy to prt-ec2)

| File | Purpose |
|---|---|
| `kafka-producer.py` | Generates and sends stock data to MSK |
| `kafka-consumer.py` | Reads messages from MSK (for testing) |

Both scripts use `OAUTHBEARER` + `SASL_SSL` with `MSKTokenProvider(AbstractTokenProvider)`.

---

## Config Files

| File | Deploy to |
|---|---|
| `config/iam-auth.properties` | `~/kafka/config/iam-auth.properties` on prt-ec2 |
| `config/connector.properties` | Reference only — paste into MSK Connect console |

---

## IAM Policy Files

All files in `iam-policies/` are ready to paste — replace `<ACCOUNT-ID>` before use.

| File | Where it goes |
|---|---|
| `msk-cluster-policy.json` | MSK → cluster → Properties → Edit cluster policy |
| `msk-connect-trust-policy.json` | IAM → msk-connect-role → trust relationship |
| `msk-connect-network-policy.json` | IAM → msk-connect-role → inline policy |
| `s3-bucket-policy.json` | S3 → bucket → Permissions → Bucket policy |
| `snowflake-trust-policy.json` | IAM → snowflake-s3-role → trust relationship |
