# 🔥 Real-Time Streaming Data Pipeline: Amazon MSK → S3 → Snowflake

> **Complete Step-by-Step Implementation Guide with Troubleshooting**

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Time Estimates](#time-estimates)
5. [Step 1 — VPC & Networking Setup](#step-1--vpc--networking-setup)
6. [Step 2 — Security Groups](#step-2--security-groups)
7. [Step 3 — EC2 Instances](#step-3--ec2-instances)
8. [Step 4 — Amazon MSK Cluster](#step-4--amazon-msk-cluster)
9. [Step 5 — Kafka Topic & Producer/Consumer](#step-5--kafka-topic--producerconsumer)
10. [Step 6 — S3 Bucket & IAM Roles](#step-6--s3-bucket--iam-roles)
11. [Step 7 — MSK Connect + S3 Sink Connector](#step-7--msk-connect--s3-sink-connector)
12. [Step 8 — Snowflake Setup & Snowpipe](#step-8--snowflake-setup--snowpipe)
13. [Step 9 — End-to-End Validation](#step-9--end-to-end-validation)
14. [Troubleshooting Decision Tree](#troubleshooting-decision-tree)
15. [Problems Faced & Solutions](#problems-faced--solutions)
16. [Cost Optimization Tips](#cost-optimization-tips)
17. [Cleanup](#cleanup)

---

## Project Overview

This project implements a **real-time streaming data pipeline** that moves stock market data from a Kafka producer all the way to Snowflake for long-term queryable storage.

### What It Does
- A Python producer generates **simulated stock market data** every second
- Data is published to **Amazon MSK** (Managed Kafka) with IAM authentication
- **MSK Connect** with Confluent S3 Sink Connector automatically flushes data to **Amazon S3** as JSON files
- **Snowpipe** detects new files in S3 and automatically loads them into a **Snowflake** table within minutes
- Data is then available for **analytics and querying** in Snowflake

### Tech Stack

| Component | Service |
|---|---|
| Kafka Broker | Amazon MSK (Managed Streaming for Kafka) |
| Kafka Producer | Python (kafka-python + IAM auth) |
| Kafka Connector | MSK Connect + Confluent S3 Sink Connector v12.1.4 |
| Data Lake | Amazon S3 |
| Data Warehouse | Snowflake |
| Auto Ingestion | Snowpipe |
| Auth | AWS IAM Role-based authentication |
| Networking | AWS VPC with public/private subnets |

---

## Architecture

```
Internet
    ↓
AWS VPC (10.0.0.0/16)
┌─────────────────────────────────────────────────────┐
│                                                     │
│  AZ1 (us-east-1a)          AZ2 (us-east-1b)        │
│  ┌─────────────────┐       ┌─────────────────┐     │
│  │  Public Subnet  │       │  Public Subnet  │     │
│  │  10.0.1.0/24    │       │  10.0.2.0/24    │     │
│  │  [pub-ec2]      │       │  (standby)      │     │
│  └────────┬────────┘       └─────────────────┘     │
│           │ SSH hop                                 │
│  ┌────────▼────────┐       ┌─────────────────┐     │
│  │  Private Subnet │       │  Private Subnet │     │
│  │  10.0.3.0/24    │       │  10.0.4.0/24    │     │
│  │  [prt-ec2]      │       │                 │     │
│  │  [MSK Broker 1] │       │  [MSK Broker 2] │     │
│  └─────────────────┘       └─────────────────┘     │
└─────────────────────────────────────────────────────┘
          ↓ MSK Connect (Confluent S3 Sink)
    Amazon S3 Bucket
    (JSON files every 100 messages)
          ↓ S3 Event Notification → SQS
    Snowpipe (auto-ingest)
          ↓
    Snowflake Table
    (queryable within minutes)
```

---

## Prerequisites

### AWS Account Setup
1. Create AWS account at: https://aws.amazon.com
2. Go to **IAM → Users → Create User** with **AdministratorAccess** policy
3. Create **Access Keys** for that user:
   ```
   IAM → Users → your-user → Security credentials
   → Create access key → CLI
   → Download CSV (save this safely!)
   ```

### Install & Configure AWS CLI on Local Machine
```bash
# Mac
brew install awscli

# Windows — Download installer from: https://aws.amazon.com/cli/

# Verify installation
aws --version

# Configure with your credentials
aws configure
# AWS Access Key ID:     <from CSV downloaded above>
# AWS Secret Access Key: <from CSV downloaded above>
# Default region name:   us-east-1
# Default output format: json

# Verify it works
aws sts get-caller-identity
# Should show your account ID and user ARN
```

### Snowflake Free Trial Setup
1. Go to: https://signup.snowflake.com
2. Fill in details and select:
   - **Cloud Provider:** `AWS`  ← Must match your AWS region
   - **Region:** `US East (N. Virginia)` ← Must be us-east-1
3. Check your email → Activate account
4. Note down your Account URL, Username & Password

### Other Requirements
- Terminal with SSH support (Mac/Linux: built-in, Windows: Git Bash or PuTTY)
- Python 3.9+ (runs on EC2, not needed locally)

---

## Time Estimates

| Step | Task | Time |
|---|---|---|
| Step 1 | VPC + Networking | ~15 min |
| Step 2 | Security Groups | ~10 min |
| Step 3 | EC2 Instances | ~10 min |
| Step 4 | MSK Cluster | ~25 min (includes 15-20 min wait) |
| Step 5 | Kafka Setup | ~15 min |
| Step 6 | S3 + IAM | ~15 min |
| Step 7 | MSK Connect | ~30 min (includes wait + troubleshooting) |
| Step 8 | Snowflake | ~20 min |
| Step 9 | Validation | ~10 min |
| **Total** | | **~2.5 hours** |

---

## Step 1 — VPC & Networking Setup

### 1.1 Create VPC
```
AWS Console → Search "VPC" in top search bar
→ Your VPCs (left sidebar)
→ Create VPC (orange button top right)

Fill in:
  Name tag:        kafka-snowflake-vpc
  IPv4 CIDR block: 10.0.0.0/16
  Tenancy:         Default

→ Click "Create VPC"
```

### 1.2 Create 4 Subnets

```
VPC Console → Subnets (left sidebar) → Create subnet
Select VPC: kafka-snowflake-vpc
```

Create these one by one:

| Name | Type | AZ | CIDR |
|---|---|---|---|
| public-subnet-1a | Public | us-east-1a | 10.0.1.0/24 |
| public-subnet-1b | Public | us-east-1b | 10.0.2.0/24 |
| private-subnet-1a | Private | us-east-1a | 10.0.3.0/24 |
| private-subnet-1b | Private | us-east-1b | 10.0.4.0/24 |

> ⚠️ After creating each **public subnet**:
> ```
> Select the subnet → Actions → Edit subnet settings
> → ✅ Enable "Auto-assign public IPv4 address" → Save
> ```

### 1.3 Create & Attach Internet Gateway
```
VPC → Internet Gateways → Create Internet Gateway
  Name: kafka-igw → Create

Select kafka-igw → Actions → Attach to VPC
→ Select: kafka-snowflake-vpc → Attach
```

### 1.4 Public Route Table
```
VPC → Route Tables → Create Route Table
  Name: public-rt
  VPC:  kafka-snowflake-vpc → Create

Select public-rt:
  → Routes tab → Edit routes → Add route:
      Destination: 0.0.0.0/0
      Target:      Internet Gateway → kafka-igw → Save

  → Subnet associations tab → Edit:
      ✅ public-subnet-1a
      ✅ public-subnet-1b → Save
```

### 1.5 NAT Gateway
```
VPC → NAT Gateways → Create NAT Gateway
  Name:              kafka-nat-gw
  Subnet:            public-subnet-1a   ← MUST be PUBLIC subnet
  Connectivity type: Public
  → Click "Allocate Elastic IP"
→ Create NAT Gateway

⏳ Wait ~2 minutes until Status = Available
```

### 1.6 Private Route Table
```
VPC → Route Tables → Create Route Table
  Name: private-rt
  VPC:  kafka-snowflake-vpc → Create

Select private-rt:
  → Routes tab → Edit routes → Add route:
      Destination: 0.0.0.0/0
      Target:      NAT Gateway → kafka-nat-gw → Save

  → Subnet associations tab → Edit:
      ✅ private-subnet-1a
      ✅ private-subnet-1b → Save
```

### 1.7 S3 VPC Endpoint (Gateway)
```
VPC → Endpoints → Create Endpoint
  Name:             s3-vpc-endpoint
  Service category: AWS Services
  Search:           s3
  Select:           com.amazonaws.us-east-1.s3  (Type: Gateway)
  VPC:              kafka-snowflake-vpc
  Route tables:     ✅ private-rt
  Policy:           Full access (default)
→ Create Endpoint
```
> 💡 This allows MSK Connect to reach S3 **privately** — faster and free

---

## Step 2 — Security Groups

> 💡 **What are Security Groups?**
> Security Groups are **firewall rules** created in VPC and **attached** to resources (EC2, MSK, MSK Connect) later. Create them all here first, attach them later.

```
Navigate to: AWS Console → VPC → Security Groups (left sidebar) → Create security group
```

### ⚠️ Security Group Naming — Use EXACTLY These Names

The exact names below are referenced throughout this entire guide. Wrong names = confusion when attaching to resources.

```
Resource            SG Name to Use
─────────────────────────────────────────
pub-ec2         →   sg-pub-ec2
prt-ec2         →   sg-prt-ec2
MSK Cluster     →   sg-msk
MSK Connector   →   mskconnect-sg   ← Note: NOT sg-mskconnect
```

---

### SG 1: `sg-pub-ec2`
```
Name:        sg-pub-ec2
Description: Security group for public bastion EC2
VPC:         kafka-snowflake-vpc
```
| Direction | Type | Protocol | Port | Source |
|---|---|---|---|---|
| Inbound | SSH | TCP | 22 | My IP |
| Outbound | All traffic | All | All | 0.0.0.0/0 |

---

### SG 2: `sg-prt-ec2`
```
Name:        sg-prt-ec2
Description: Security group for private Kafka client EC2
VPC:         kafka-snowflake-vpc
```
| Direction | Type | Protocol | Port | Source |
|---|---|---|---|---|
| Inbound | SSH | TCP | 22 | sg-pub-ec2 |
| Inbound | All traffic | All | All | sg-pub-ec2 |
| Outbound | All traffic | All | All | 0.0.0.0/0 |

> ⚠️ To use another SG as source: type `sg-pub-ec2` in the Source dropdown and select it from the list

---

### SG 3: `sg-msk`
```
Name:        sg-msk
Description: Security group for MSK Kafka brokers
VPC:         kafka-snowflake-vpc
```
| Direction | Type | Protocol | Port | Source |
|---|---|---|---|---|
| Inbound | Custom TCP | TCP | 9098 | sg-prt-ec2 |
| Inbound | Custom TCP | TCP | 9098 | mskconnect-sg |
| Inbound | Custom TCP | TCP | 9092 | sg-prt-ec2 |
| Inbound | Custom TCP | TCP | 2181 | sg-prt-ec2 |
| Inbound | All traffic | All | All | sg-msk (self-reference) |
| Outbound | All traffic | All | All | 0.0.0.0/0 |

> ⚠️ The self-referencing rule (sg-msk as its own source) allows MSK brokers to communicate with each other

---

### SG 4: `mskconnect-sg`

> ⚠️ **Important:** Named `mskconnect-sg` NOT `sg-mskconnect`. Use this exact name everywhere.

```
Name:        mskconnect-sg
Description: Security group for MSK Connect workers
VPC:         kafka-snowflake-vpc
```
| Direction | Type | Protocol | Port | Source |
|---|---|---|---|---|
| Inbound | None | - | - | - |
| Outbound | All traffic | All | All | 0.0.0.0/0 |

---

## Step 3 — EC2 Instances

### 3.1 Launch pub-ec2 (Bastion Host)
```
EC2 → Instances → Launch Instances

Name:              pub-ec2
AMI:               Amazon Linux 2023 AMI
Architecture:      64-bit (x86)
Instance type:     t2.micro

Key pair:
  → Create new key pair
  → Name: kafka-key
  → Type: RSA
  → Format: .pem (Mac/Linux)  or  .ppk (Windows PuTTY)
  → Create key pair   ← FILE AUTO-DOWNLOADS
  ⚠️ SAVE THIS FILE SAFELY — cannot download again!

Network settings → Edit:
  VPC:                    kafka-snowflake-vpc
  Subnet:                 public-subnet-1a
  Auto-assign public IP:  Enable
  Security group:         Select existing → sg-pub-ec2

Storage: 8 GiB gp3 (default)
→ Launch Instance
→ Wait: Instance State = Running, Status checks = 2/2 passed
→ Copy the Public IPv4 address
```

### 3.2 Launch prt-ec2 (Private Kafka Client)
```
EC2 → Instances → Launch Instances

Name:              prt-ec2
AMI:               Amazon Linux 2023 AMI
Architecture:      64-bit (x86)
Instance type:     t2.micro
Key pair:          kafka-key  ← SAME KEY as pub-ec2

Network settings → Edit:
  VPC:                    kafka-snowflake-vpc
  Subnet:                 private-subnet-1a
  Auto-assign public IP:  Disable   ← No public IP
  Security group:         Select existing → sg-prt-ec2

Storage: 8 GiB gp3 (default)
→ Launch Instance
→ Wait: Instance State = Running
→ Copy the Private IPv4 address (e.g. 10.0.3.xxx)
```

### 3.3 SSH Connection (Two Hops Required)

```
Your Machine → [Internet] → pub-ec2 (public IP)
                                  ↓ [VPC private network]
                             prt-ec2 (private IP only)
```

```bash
# ── On LOCAL machine ────────────────────────────────────
# Fix permissions (Mac/Linux only)
chmod 400 ~/Downloads/kafka-project-key.pem

# SSH into pub-ec2
ssh -i ~/Downloads/kafka-project-key.pem ec2-user@<pub-ec2-PUBLIC-IP>

# ── On LOCAL machine (new terminal tab) ─────────────────
# Copy .pem key to pub-ec2 (needed to SSH into prt-ec2)
scp -i ~/Downloads/kafka-project-key.pem \
    ~/Downloads/kafka-project-key.pem \
    ec2-user@<pub-ec2-PUBLIC-IP>:~/.ssh/kafka-project-key.pem

# ── On pub-ec2 terminal ─────────────────────────────────
chmod 400 ~/.ssh/kafka-project-key.pem

# SSH from pub-ec2 into prt-ec2
ssh -i ~/.ssh/kafka-project-key.pem ec2-user@<prt-ec2-PRIVATE-IP>
# Prompt should show: [ec2-user@ip-10-0-3-xxx ~]$
```

### 3.4 Install Java & Kafka on prt-ec2
```bash
sudo yum update -y
sudo yum install java-11-amazon-corretto -y
java -version   # Verify: openjdk version "11.x.x"

wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
mv kafka_2.13-3.5.1 kafka
ls ~/kafka   # Should show: bin  config  libs  licenses  logs
```

### 3.5 Create IAM Role for EC2
```
IAM → Roles → Create Role
  Trusted entity type: AWS Service
  Use case:            EC2 → Next

  Attach policies:
    ✅ AmazonMSKFullAccess
    ✅ AmazonS3FullAccess
    ✅ CloudWatchFullAccess
  → Next

  Role name: ec2-msk-role → Create Role
```

Attach to **both EC2 instances**:
```
EC2 → Instances → Select prt-ec2
→ Actions → Security → Modify IAM Role → ec2-msk-role → Update

EC2 → Instances → Select pub-ec2
→ Actions → Security → Modify IAM Role → ec2-msk-role → Update
```

Verify on prt-ec2:
```bash
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")

curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
# ✅ Expected output: ec2-msk-role
```

---

## Step 4 — Amazon MSK Cluster

### 4.1 Create Cluster
```
MSK → Clusters → Create Cluster
Creation method: Custom create   ← NOT Quick create

Cluster settings:
  Name:                 kafka-msk-cluster
  Type:                 Provisioned
  Kafka version:        3.5.1
  Broker type:          kafka.t3.small
  Number of AZs:        2
  Brokers per AZ:       1

Storage:
  EBS storage per broker: 20 GiB
```

### 4.2 Networking
```
VPC:    kafka-snowflake-vpc

First AZ:   us-east-1a → private-subnet-1a
Second AZ:  us-east-1b → private-subnet-1b

Security groups:
  Remove default SG
  Add: sg-msk
```

### 4.3 Security Settings
```
Access Control Methods:
  ✅ IAM role-based authentication   ← MUST enable
  ☐ Unauthenticated access
  ☐ SASL/SCRAM

Encryption in transit: TLS encryption

→ Create cluster
⏳ Wait 15-20 minutes until Status = Active
```

### 4.4 Get Bootstrap String
```
MSK → kafka-msk-cluster → "View client information" button
→ Bootstrap servers section
→ Copy the IAM row (port 9098)

Example:
b-1.kafkamskcluster.dc3y5j.c10.kafka.us-east-1.amazonaws.com:9098,
b-2.kafkamskcluster.dc3y5j.c10.kafka.us-east-1.amazonaws.com:9098
```

> ⚠️ Copy the **IAM** string (port **9098**), NOT plaintext (9092)

### 4.5 Save Bootstrap as Environment Variable

```bash
# On prt-ec2 — add to .bashrc for persistence

nano ~/.bashrc

# Add at the bottom (replace with YOUR actual string):
export BOOTSTRAP="YOUR-BOOTSTRAP-STRING"

Example: export BOOTSTRAP="b-1.kafkghFskcluster.dc3y5j.c10.kafka.us-east-1.amazonaws.com:9098,b-2.kafkamskcluster.dA4y5j.c10.kafka.us-east-1.amazonaws.com:9098"

# Ctrl+X → Y → Enter to save

source ~/.bashrc

# Verify
echo $BOOTSTRAP
# ✅ Should print full bootstrap string
```

### 4.6 MSK Cluster Access Policy

```
MSK → kafka-msk-cluster → Properties tab
→ Security settings → Edit cluster policy
→ Paste (replace <ACCOUNT-ID>):
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::<ACCOUNT-ID>:role/msk-connect-role",
          "arn:aws:iam::<ACCOUNT-ID>:role/ec2-msk-role"
        ]
      },
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:<ACCOUNT-ID>:cluster/kafka-msk-cluster/*",
        "arn:aws:kafka:us-east-1:<ACCOUNT-ID>:topic/kafka-msk-cluster/*/*",
        "arn:aws:kafka:us-east-1:<ACCOUNT-ID>:group/kafka-msk-cluster/*/*"
      ]
    }
  ]
}
```

### 4.7 Configure IAM Auth on prt-ec2
```bash
cat > ~/kafka/config/iam-auth.properties << 'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

cd ~/kafka/libs
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar
```

### 4.8 Test Connection
```bash
cd ~/kafka
bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP \
  --command-config config/iam-auth.properties \
  --list

# ✅ Empty output = SUCCESS
# ❌ "Connection refused" = Check sg-msk inbound rules (port 9098 from sg-prt-ec2)
# ❌ "Authentication failed" = Check IAM role attached to prt-ec2
```

---

## Step 5 — Kafka Topic & Producer/Consumer

### 5.1 Create Topic
```bash
cd ~/kafka
bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP \
  --command-config config/iam-auth.properties \
  --create \
  --topic stock-market-data \
  --partitions 3 \
  --replication-factor 2
# ✅ Expected: Created topic stock-market-data.
```

### 5.2 Install Python Dependencies
```bash
sudo yum install python3-pip -y
pip3 install kafka-python boto3 aws-msk-iam-sasl-signer-python
pip3 show kafka-python   # Verify version 2.0.0+
```

### 5.3 Producer Script
```bash
nano ~/kafka_producer.py
```

```python
import json, random, time
from datetime import datetime
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider      # ← Critical import
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# ⚠️ MUST inherit AbstractTokenProvider
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

BOOTSTRAP = "YOUR-BOOTSTRAP-STRING:9098"   # ← Replace this

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA']
count = 0
print("🚀 Starting Producer... Ctrl+C to stop")
while True:
    stock = random.choice(STOCKS)
    msg = {
        "symbol": stock,
        "price": round(random.uniform(100, 1500), 2),
        "volume": random.randint(1000, 50000),
        "change": round(random.uniform(-5.0, 5.0), 2),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "exchange": "NASDAQ"
    }
    producer.send('stock-market-data', key=stock, value=msg)
    count += 1
    print(f"✅ Sent [{count}]: {msg}")
    time.sleep(1)
```

### 5.4 Consumer Script
```bash
nano ~/kafka_consumer.py
```

```python
import json
from kafka import KafkaConsumer
from kafka.sasl.oauth import AbstractTokenProvider      # ← Critical import
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# ⚠️ MUST inherit AbstractTokenProvider
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

BOOTSTRAP = "YOUR-BOOTSTRAP-STRING:9098"   # ← Replace this

consumer = KafkaConsumer(
    'stock-market-data',
    bootstrap_servers=BOOTSTRAP,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    auto_offset_reset='earliest',
    group_id='stock-consumer-group'
)

print("👂 Listening... Ctrl+C to stop")
for msg in consumer:
    print(f"📨 Partition:{msg.partition} Offset:{msg.offset} | {msg.value}")
```

### 5.5 Run Both Scripts
```bash
# Terminal 1 — Consumer FIRST (leave running)
python3 ~/kafka_consumer.py

# Terminal 2 — Then Producer
python3 ~/kafka_producer.py

# Stop both when verified
Ctrl+C
```

---

## Step 6 — S3 Bucket & IAM Roles

### 6.1 Create S3 Bucket
```
S3 → Create Bucket
  Bucket name:          kafka-msk-snowflake-bucket
  Region:               us-east-1   ← Must match MSK region
  Block all public access: ✅ ON (default)
→ Create Bucket
```

### 6.2 Create Folder Structure
```
Inside kafka-msk-snowflake-bucket:
  → Create folder: topics
      → Inside topics, create folder: stock-market-data
  → Back at root, create folder: plugins

Result:
  kafka-msk-snowflake-bucket/
    ├── topics/stock-market-data/   ← JSON files land here
    └── plugins/                    ← Connector ZIPs go here
```

### 6.3 Create MSK Connect IAM Role

> ⚠️ Use **"Custom trust policy"** — NOT the MSK Connect service use case option

```
IAM → Roles → Create Role → Custom trust policy
```

Trust policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "kafkaconnect.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
```

```
→ Next
Attach: ✅ AmazonS3FullAccess
        ✅ AmazonMSKFullAccess
        ✅ CloudWatchFullAccess
→ Next
Role name: msk-connect-role
→ Create Role
```

Then add inline policy:
```
IAM → msk-connect-role → Permissions → Add permissions → Create inline policy → JSON
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["ec2:CreateNetworkInterface"],
      "Resource": "arn:aws:ec2:*:*:network-interface/*",
      "Condition": {
        "StringEquals": {"aws:RequestTag/AmazonMSKConnectManaged": "true"},
        "ForAllValues:StringEquals": {"aws:TagKeys": "AmazonMSKConnectManaged"}
      }
    },
    {
      "Effect": "Allow",
      "Action": ["ec2:CreateNetworkInterface"],
      "Resource": ["arn:aws:ec2:*:*:subnet/*", "arn:aws:ec2:*:*:security-group/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["ec2:CreateTags"],
      "Resource": "arn:aws:ec2:*:*:network-interface/*",
      "Condition": {"StringEquals": {"ec2:CreateAction": "CreateNetworkInterface"}}
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeNetworkInterfaces",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:AttachNetworkInterface",
        "ec2:DetachNetworkInterface",
        "ec2:DeleteNetworkInterface"
      ],
      "Resource": "arn:aws:ec2:*:*:network-interface/*",
      "Condition": {"StringEquals": {"ec2:ResourceTag/AmazonMSKConnectManaged": "true"}}
    }
  ]
}
```

```
Policy name: msk-connect-network-policy → Create Policy
```

### 6.4 Add S3 Bucket Policy
```
S3 → kafka-msk-snowflake-bucket → Permissions tab
→ Bucket policy → Edit → Delete existing → Paste (replace <ACCOUNT-ID>):
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ACCOUNT-ID>:role/msk-connect-role"
      },
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::kafka-msk-snowflake-bucket",
        "arn:aws:s3:::kafka-msk-snowflake-bucket/*"
      ]
    }
  ]
}
```

---

## Step 7 — MSK Connect + S3 Sink Connector

### 7.1 Download Confluent S3 Connector Plugin

> ⚠️ **Before downloading — read these critical notes:**
> - Plugin ZIP must be **20MB or larger** — a 130KB file is just a JAR and won't work
> - **prt-ec2 cannot reach Cloudfront CDN** — download on pub-ec2 or local machine instead
> - Version used in this project: `confluentinc-kafka-connect-s3-12.1.4.zip` (~75MB)

#### Download on pub-ec2:
```bash
# SSH into pub-ec2 (has public internet)
ssh -i ~/Downloads/kafka-project-key.pem ec2-user@<pub-ec2-PUBLIC-IP>

# Try GitHub release (no login needed)
wget https://github.com/confluentinc/kafka-connect-storage-cloud/releases/download/v10.5.9/confluentinc-kafka-connect-s3-10.5.9.zip

# Verify size MUST be 20MB+
ls -lh confluentinc-kafka-connect-s3-*.zip

# Upload to S3
aws s3 cp ~/confluentinc-kafka-connect-s3-*.zip \
  s3://kafka-msk-snowflake-bucket/plugins/

# Verify
aws s3 ls s3://kafka-msk-snowflake-bucket/plugins/
# Must show file size of 20MB+ — if 130KB, re-download the correct file
```

#### OR download on local machine:
1. Go to: https://www.confluent.io/hub/confluentinc/kafka-connect-s3
2. Click **Download** (free account required)
3. Verify file is ~20-75MB
4. Upload via S3 Console:
   ```
   S3 → kafka-msk-snowflake-bucket → plugins/ → Upload → Add files → Select ZIP → Upload
   ```

### 7.2 Create CloudWatch Log Group FIRST

> ⚠️ **Must create BEFORE connector** — connector fails immediately if log group missing

```
CloudWatch → Log Groups (left sidebar) → Create log group
  Log group name: /msk-connect/msk-s3-sink-connector
  Retention:      1 week
→ Create

Verify: CloudWatch → Log Groups → Search "msk-connect"
→ Should show: /msk-connect/msk-s3-sink-connector ✅
```

### 7.3 Create Custom Plugin
```
MSK → MSK Connect → Custom plugins → Create custom plugin
  S3 URI:       s3://kafka-msk-snowflake-bucket/plugins/confluentinc-kafka-connect-s3-12.1.4.zip
  Plugin name:  confluent-s3-sink-plugin
  Content type: ZIP
→ Create
⏳ Wait 2-3 minutes until Status = Active
```

### 7.4 Create Connector

```
MSK Connect → Connectors → Create connector
→ Select: confluent-s3-sink-plugin ✅ → Next
```

**Connector name:** `msk-s3-sink-connector`

**Connector configuration** (paste exactly — no quotes, no braces):
```properties
connector.class=io.confluent.connect.s3.S3SinkConnector
tasks.max=1
topics=stock-market-data
s3.region=us-east-1
s3.bucket.name=kafka-msk-snowflake-bucket
s3.part.size=5242880
topics.dir=topics
flush.size=100
storage.class=io.confluent.connect.s3.storage.S3Storage
format.class=io.confluent.connect.s3.format.json.JsonFormat
schema.compatibility=NONE
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
key.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
locale=en_US
timezone=UTC
timestamp.extractor=WallClock
rotate.interval.ms=60000
rotate.schedule.interval.ms=60000
errors.retry.timeout=-1
errors.retry.delay.max.ms=60000
errors.tolerance=all
errors.log.enable=true
errors.log.include.messages=true
```

**Remaining settings:**
```
Kafka Connect version: 3.7.x (recommended)
MSK Cluster:           kafka-msk-cluster
Authentication:        IAM
VPC:                   kafka-snowflake-vpc
Subnets:               ✅ private-subnet-1a  ✅ private-subnet-1b
Security groups:       mskconnect-sg
IAM role:              msk-connect-role
Capacity type:         Provisioned
MCU count:             1
Workers:               1
CloudWatch Logs:       ✅ Enable
Log group:             /msk-connect/msk-s3-sink-connector

→ Create connector
⏳ Wait 5-10 minutes until Status = Running
```

### 7.5 How to Delete & Recreate a Connector

> ⚠️ **You CANNOT edit a connector after creation.** To fix any issue, you must DELETE and CREATE a new one.

```
MSK Connect → Connectors → Select connector
→ Actions → Delete connector → Confirm name → Delete
⏳ Wait 1-2 min for deletion → Then create new connector
```

### 7.6 Verify Files in S3
```bash
# Start producer first
python3 ~/kafka_producer.py

# After 2-3 minutes check S3
aws s3 ls s3://kafka-msk-snowflake-bucket/topics/stock-market-data/ --recursive

# ✅ Expected files:
# topics/stock-market-data/partition=0/stock-market-data+0+0000000000.json
# topics/stock-market-data/partition=1/stock-market-data+1+0000000000.json
# topics/stock-market-data/partition=2/stock-market-data+2+0000000000.json
```

---

## Step 8 — Snowflake Setup & Snowpipe

### 8.1 Open SQL Worksheet
```
https://app.snowflake.com → Login
→ Worksheets (left sidebar) → "+" → SQL Worksheet
```

### 8.2 Create Database & Table
```sql
CREATE DATABASE KAFKA_DB;
USE DATABASE KAFKA_DB;
CREATE SCHEMA KAFKA_SCHEMA;
USE SCHEMA KAFKA_SCHEMA;

CREATE OR REPLACE TABLE STOCK_MARKET_DATA (
    symbol      VARCHAR(10),
    price       FLOAT,
    volume      INTEGER,
    change      FLOAT,
    timestamp   VARCHAR(30),
    exchange    VARCHAR(20),
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

DESCRIBE TABLE STOCK_MARKET_DATA;   -- Verify
```

### 8.3 Create Storage Integration
```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_kafka_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT-ID>:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://kafka-msk-snowflake-bucket/topics/stock-market-data/');

DESC INTEGRATION s3_kafka_integration;
-- ✅ Copy these two values for next step:
--   STORAGE_AWS_IAM_USER_ARN  → arn:aws:iam::XXXX:user/XXXX
--   STORAGE_AWS_EXTERNAL_ID   → XXXX_SFCRole=XXXX
```

### 8.4 Create Snowflake IAM Role in AWS
```
IAM → Roles → Create Role → Custom trust policy
Paste (replace placeholders with values from DESC INTEGRATION above):
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"}
      }
    }
  ]
}
```

```
→ Next → Attach: ✅ AmazonS3FullAccess
→ Role name: snowflake-s3-role → Create Role
```

### 8.5 Create Stage, Load Data & Snowpipe
```sql
-- Create external stage
CREATE OR REPLACE STAGE kafka_s3_stage
    URL = 's3://kafka-msk-snowflake-bucket/topics/stock-market-data/'
    STORAGE_INTEGRATION = s3_kafka_integration
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE);

LIST @kafka_s3_stage;   -- Verify S3 files visible

-- Test manual load
COPY INTO STOCK_MARKET_DATA (symbol, price, volume, change, timestamp, exchange)
FROM (
    SELECT $1:symbol::VARCHAR, $1:price::FLOAT, $1:volume::INTEGER,
           $1:change::FLOAT, $1:timestamp::VARCHAR, $1:exchange::VARCHAR
    FROM @kafka_s3_stage
)
FILE_FORMAT = (TYPE = 'JSON') ON_ERROR = 'CONTINUE';

SELECT * FROM STOCK_MARKET_DATA LIMIT 10;   -- Verify data loaded

-- Create Snowpipe for auto-ingestion
CREATE OR REPLACE PIPE kafka_snowpipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO STOCK_MARKET_DATA (symbol, price, volume, change, timestamp, exchange)
    FROM (
        SELECT $1:symbol::VARCHAR, $1:price::FLOAT, $1:volume::INTEGER,
               $1:change::FLOAT, $1:timestamp::VARCHAR, $1:exchange::VARCHAR
        FROM @kafka_s3_stage
    )
    FILE_FORMAT = (TYPE = 'JSON');

SHOW PIPES;   -- Copy notification_channel ARN for next step
```

### 8.6 Configure S3 Event Notification
```
S3 → kafka-msk-snowflake-bucket → Properties tab
→ Event notifications → Create event notification
  Name:         snowpipe-trigger
  Prefix:       topics/stock-market-data/
  Event types:  ✅ s3:ObjectCreated:*
  Destination:  SQS Queue
  SQS ARN:      <paste notification_channel from SHOW PIPES>
→ Save changes
```

---

## Step 9 — End-to-End Validation

```bash
# Start producer
python3 ~/kafka_producer.py
```

```bash
# Check Kafka
cd ~/kafka
bin/kafka-consumer-groups.sh \
  --bootstrap-server $BOOTSTRAP \
  --command-config config/iam-auth.properties \
  --describe --group stock-consumer-group
# ✅ LAG = 0 means data flowing
```

```bash
# Check S3
aws s3 ls s3://kafka-msk-snowflake-bucket/topics/stock-market-data/ --recursive
# ✅ New JSON files appearing
```

```sql
-- Check Snowflake
SELECT SYSTEM$PIPE_STATUS('kafka_snowpipe');  -- executionState = RUNNING
SELECT COUNT(*) FROM STOCK_MARKET_DATA;       -- Growing number
SELECT SYMBOL, COUNT(*) AS RECORDS, AVG(PRICE) AS AVG_PRICE
FROM STOCK_MARKET_DATA GROUP BY SYMBOL ORDER BY SYMBOL;
```

---

## Troubleshooting Decision Tree

```
Connector Failed?
      │
      ▼
Check MSK Connect Console → Connector → Status
      │
      ├─► "InvalidInput.WorkerLogsError"
      │         Log group doesn't exist
      │         Fix: Create CloudWatch log group first (Step 7.2)
      │              Then delete & recreate connector
      │
      ├─► "KafkaConnect.BrokerAuthenticationFailure"
      │         msk-connect-role not in MSK cluster policy
      │         Fix: Add cluster policy (Step 4.6)
      │
      ├─► No error in console? → Check CloudWatch logs
      │         CloudWatch → Log Groups
      │         → /msk-connect/msk-s3-sink-connector
      │         → Filter: ERROR
      │              │
      │              ├─► "Class not found: io.confluent..."
      │              │     Wrong class name — use Confluent class
      │              │     with Confluent plugin
      │              │
      │              ├─► "s3:PutObject denied"
      │              │     Add S3 bucket policy (Step 6.4)
      │              │
      │              ├─► "DeprecationConfigDefProcessor"
      │              │     Using Lenses v8 with old KCQL syntax
      │              │     Switch to Confluent connector entirely
      │              │
      │              └─► "HTTP 400" on POST /connectors
      │                    Config properties rejected
      │                    Use key=value format, no { } braces
      │
      └─► Connector Running but no S3 files?
                Start the producer script first!
                python3 ~/kafka_producer.py
```

### Quick Error Reference

| Error | Root Cause | Fix |
|---|---|---|
| `InvalidInput.WorkerLogsError` | Log group missing | Create CloudWatch log group first |
| `BrokerAuthenticationFailure` | MSK cluster policy | Add msk-connect-role to cluster policy |
| `s3:PutObject denied` | S3 bucket policy | Add explicit bucket policy |
| `Class not found: io.confluent` | Plugin/config mismatch | Match class to loaded plugin |
| `DeprecationConfigDefProcessor` | Lenses v8 KCQL removed | Switch to Confluent connector |
| `NoBrokersAvailable` | Wrong bootstrap port | Use port 9098 not 9092 |
| `AssertionError: AbstractTokenProvider` | Missing inheritance | Inherit from AbstractTokenProvider |
| `401 Unauthorized` on metadata | IMDSv2 required | Use TOKEN=$(curl PUT ...) approach |
| `Unable to locate credentials` | IAM role not attached | Attach ec2-msk-role to EC2 instance |

---

## Problems Faced & Solutions

### Problem 1 — IMDSv2 Token Required (401 Error)
**Error:** `curl http://169.254.169.254/...` → `401 Unauthorized`

**Cause:** EC2 uses IMDSv2 requiring a session token
```bash
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

### Problem 2 — Kafka Consumer AssertionError
**Error:** `AssertionError: sasl_oauth_token_provider must implement AbstractTokenProvider`

**Cause:** Missing class inheritance
```python
# ❌ Wrong
class MSKTokenProvider:
# ✅ Correct
from kafka.sasl.oauth import AbstractTokenProvider
class MSKTokenProvider(AbstractTokenProvider):
```

### Problem 3 — IAM Role Not Attached
**Error:** Blank output or `Unable to locate credentials`

**Fix:** `EC2 → prt-ec2 → Actions → Security → Modify IAM Role → ec2-msk-role`

### Problem 4 — MSK Connect Log Group Missing
**Error:** `Code: InvalidInput.WorkerLogsError`

**Fix:** Create CloudWatch log group BEFORE creating connector

### Problem 5 — Wrong Connector Class Name
**Error:** `Failed to find any class that matches io.confluent.connect.s3.S3SinkConnector`

**Fix:** Use `io.confluent...` class ONLY with Confluent plugin, `io.lenses...` ONLY with Lenses plugin

### Problem 6 — Lenses v8 KCQL Deprecated
**Error:** `DeprecationConfigDefProcessor.createError`

**Fix:** Switch to Confluent S3 Sink Connector entirely

### Problem 7 — Plugin Download 130KB (Wrong File)
**Error:** Connector fails silently, plugin too small

**Fix:** Download full ZIP (~20MB+) from Confluent Hub website, not Maven URL

### Problem 8 — prt-ec2 Cannot Reach Cloudfront
**Error:** `wget: unable to resolve host address 'd1i4a15mxbxib1.cloudfront.net'`

**Fix:** Download on pub-ec2 (public subnet) or local machine instead

### Problem 9 — S3 PutObject Denied
**Error:** `msk-connect-role is not authorized to perform: s3:PutObject`

**Fix:** Add explicit S3 bucket policy allowing `msk-connect-role` (Step 6.4)

### Problem 10 — MSK Broker Authentication Failure
**Error:** `Code: KafkaConnect.BrokerAuthenticationFailure`

**Fix:** Add MSK cluster access policy (Step 4.6) allowing `msk-connect-role`

---

## Cost Optimization Tips

| Resource | Approx Cost | Tip |
|---|---|---|
| MSK Cluster (t3.small x2) | ~$0.12/hr | Delete when not testing |
| NAT Gateway | ~$0.045/hr + data | Delete after project — charges even idle |
| EC2 (t2.micro x2) | ~$0.024/hr | Stop when not in use |
| MSK Connect (1 MCU) | ~$0.11/hr | Delete connector when not testing |
| S3 | ~$0.023/GB/month | Add lifecycle policy to Glacier |
| CloudWatch Logs | ~$0.50/GB | Set 1 week retention |

> ⚠️ **Estimated running cost: ~$5-10/day. Always clean up after testing!**

---

## Cleanup

```
# Order matters — delete dependencies first

1. Stop producer:          Ctrl+C in producer terminal
2. Delete MSK Connector:   MSK Connect → Connectors → Delete
3. Delete MSK Cluster:     MSK → Clusters → Delete
4. Stop EC2 instances:     EC2 → Both instances → Stop/Terminate
5. Delete NAT Gateway:     VPC → NAT Gateways → Delete  ← charges even idle!
6. Release Elastic IP:     VPC → Elastic IPs → Release
7. Empty & delete S3:      S3 → Empty bucket → Delete bucket
8. Delete Log Groups:      CloudWatch → Log Groups → Delete
```

---

## Final Pipeline Summary

```
Python Producer (prt-ec2)
    ↓  1 msg/sec | IAM auth | SASL_SSL port 9098
Amazon MSK Kafka Cluster
    Topic: stock-market-data | 3 partitions | RF=2
    ↓  MSK Connect polls continuously
Confluent S3 Sink Connector
    Flushes every 100 messages as JSON
    ↓
Amazon S3: kafka-msk-snowflake-bucket
    Path: topics/stock-market-data/partition=X/
    ↓  S3 ObjectCreated event → SQS → Snowpipe
Snowpipe (Auto Ingest)
    Polls SQS, loads new files within minutes
    ↓
Snowflake: KAFKA_DB.KAFKA_SCHEMA.STOCK_MARKET_DATA
    Ready for analytics! 🎉
```

---

*Built with Danial Aneeq Ahmed — Real-time streaming pipeline: MSK → S3 → Snowflake*
*Last updated: April 2026*
