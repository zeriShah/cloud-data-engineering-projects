import boto3
import json
import time
import io
import re

# ============================================================
# CONFIG — Only change these 3 values
# ============================================================
SCRIPTS_BUCKET   = 'redshift-scripts-data-us'    # must be globally unique
PROCESSED_BUCKET = 'redshift-processed-data-us'  # must be globally unique
DB_PASSWORD      = 'Shah12345'                 # min 8 chars, 1 upper, 1 number
# ============================================================

REGION         = 'us-east-1'
WORKGROUP_NAME = 'redshift-workgroup'
NAMESPACE_NAME = 'redshift-namespace'
DB_NAME        = 'dev'
DB_USER        = 'adminuser'
SECRET_NAME    = 'redshift/etl-credentials'
GLUE_JOB_NAME  = 'myglue'
GLUE_ROLE_NAME = 'GlueETLRole'
SPECTRUM_ROLE  = 'RedshiftSpectrumRole'
VPC_CIDR       = '10.0.0.0/16'
SUBNET1_CIDR   = '10.0.1.0/24'
SUBNET2_CIDR   = '10.0.2.0/24'
AZ1            = f'{REGION}a'
AZ2            = f'{REGION}b'

ec2  = boto3.client('ec2',                  region_name=REGION)
iam  = boto3.client('iam',                  region_name=REGION)
sm   = boto3.client('secretsmanager',       region_name=REGION)
glue = boto3.client('glue',                 region_name=REGION)
s3   = boto3.client('s3',                   region_name=REGION)
rs   = boto3.client('redshift-serverless',  region_name=REGION)
sts  = boto3.client('sts',                  region_name=REGION)

ACCOUNT_ID        = sts.get_caller_identity()['Account']
SPECTRUM_ROLE_ARN = f'arn:aws:iam::{ACCOUNT_ID}:role/{SPECTRUM_ROLE}'
GLUE_ROLE_ARN     = f'arn:aws:iam::{ACCOUNT_ID}:role/{GLUE_ROLE_NAME}'

def ok(msg):    print(f"  ✅ {msg}")
def info(msg):  print(f"  ℹ️  {msg}")
def section(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

# ============================================================
# 1. VPC
# ============================================================
section("1. VPC")

vpc    = ec2.create_vpc(
    CidrBlock=VPC_CIDR,
    TagSpecifications=[{
        'ResourceType': 'vpc',
        'Tags': [{'Key': 'Name', 'Value': 'etl-vpc'}]
    }]
)['Vpc']
vpc_id = vpc['VpcId']
ok(f"VPC created: {vpc_id}")

# FIX: Enable DNS — required for all Interface VPC Endpoints to work
ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={'Value': True})
ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={'Value': True})
ok("DNS Support + DNS Hostnames enabled")

# ============================================================
# 2. SUBNETS
# ============================================================
section("2. SUBNETS")

subnet1    = ec2.create_subnet(
    VpcId=vpc_id, CidrBlock=SUBNET1_CIDR, AvailabilityZone=AZ1,
    TagSpecifications=[{'ResourceType': 'subnet',
        'Tags': [{'Key': 'Name', 'Value': 'etl-private-subnet-1'}]}]
)['Subnet']
subnet1_id = subnet1['SubnetId']
ok(f"Subnet 1: {subnet1_id} ({AZ1})")

subnet2    = ec2.create_subnet(
    VpcId=vpc_id, CidrBlock=SUBNET2_CIDR, AvailabilityZone=AZ2,
    TagSpecifications=[{'ResourceType': 'subnet',
        'Tags': [{'Key': 'Name', 'Value': 'etl-private-subnet-2'}]}]
)['Subnet']
subnet2_id = subnet2['SubnetId']
ok(f"Subnet 2: {subnet2_id} ({AZ2})")

# ============================================================
# 3. ROUTE TABLE
# ============================================================
section("3. ROUTE TABLE")

rt    = ec2.create_route_table(
    VpcId=vpc_id,
    TagSpecifications=[{'ResourceType': 'route-table',
        'Tags': [{'Key': 'Name', 'Value': 'etl-private-rt'}]}]
)['RouteTable']
rt_id = rt['RouteTableId']
ok(f"Route table: {rt_id}")

ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet1_id)
ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet2_id)
ok("Both subnets associated with route table")

# ============================================================
# 4. SECURITY GROUP
# FIX: Add ALL 3 rules at creation time:
#   - All traffic self-ref  → required by Glue workers
#   - Port 5439 self-ref    → required for Redshift
#   - Port 443 self-ref     → required for Interface VPC Endpoints
# ============================================================
section("4. SECURITY GROUP")

sg    = ec2.create_security_group(
    GroupName='etl-redshift-sg',
    Description='ETL project - Glue and Redshift',
    VpcId=vpc_id,
    TagSpecifications=[{'ResourceType': 'security-group',
        'Tags': [{'Key': 'Name', 'Value': 'etl-redshift-sg'}]}]
)
sg_id = sg['GroupId']
ok(f"Security group: {sg_id}")

ec2.authorize_security_group_ingress(
    GroupId=sg_id,
    IpPermissions=[
        {   # FIX: All traffic self-ref — Glue workers need this
            'IpProtocol': '-1',
            'UserIdGroupPairs': [{'GroupId': sg_id}]
        },
        {   # FIX: Port 443 — Interface VPC Endpoints need this
            'IpProtocol': 'tcp', 'FromPort': 443, 'ToPort': 443,
            'UserIdGroupPairs': [{'GroupId': sg_id}]
        },
        {   # Port 5439 — Redshift
            'IpProtocol': 'tcp', 'FromPort': 5439, 'ToPort': 5439,
            'UserIdGroupPairs': [{'GroupId': sg_id}]
        }
    ]
)
ok("Inbound rules: All traffic (self) + port 443 (self) + port 5439 (self)")

# ============================================================
# 5. VPC ENDPOINTS
# FIX: S3 Gateway uses route table (not subnets)
# FIX: Interface endpoints need PrivateDnsEnabled=True
# FIX: All Interface endpoints need the security group
# ============================================================
section("5. VPC ENDPOINTS")

# S3 Gateway — uses route table, not subnets or security groups
s3_ep = ec2.create_vpc_endpoint(
    VpcEndpointType='Gateway',
    VpcId=vpc_id,
    ServiceName=f'com.amazonaws.{REGION}.s3',
    RouteTableIds=[rt_id],   # FIX: must be associated with route table
    TagSpecifications=[{'ResourceType': 'vpc-endpoint',
        'Tags': [{'Key': 'Name', 'Value': 'etl-s3-endpoint'}]}]
)['VpcEndpoint']
ok(f"S3 Gateway endpoint: {s3_ep['VpcEndpointId']}")

# Interface endpoints — STS, Glue, Secrets Manager
for svc, tag in [
    ('sts',            'etl-sts-endpoint'),
    ('glue',           'etl-glue-endpoint'),
    ('secretsmanager', 'etl-secretsmanager-endpoint'),
]:
    ep = ec2.create_vpc_endpoint(
        VpcEndpointType='Interface',
        VpcId=vpc_id,
        ServiceName=f'com.amazonaws.{REGION}.{svc}',
        SubnetIds=[subnet1_id, subnet2_id],
        SecurityGroupIds=[sg_id],
        PrivateDnsEnabled=True,
        TagSpecifications=[{'ResourceType': 'vpc-endpoint',
            'Tags': [{'Key': 'Name', 'Value': tag}]}]
    )['VpcEndpoint']
    ok(f"{svc.upper()} Interface endpoint: {ep['VpcEndpointId']}")

info("Waiting 20s for endpoints to become available...")
time.sleep(20)

# ============================================================
# 6. IAM ROLES
# FIX: Added AmazonRedshiftDataFullAccess to GlueETLRole
#      (required for Redshift Data API used in rs_query.py)
# FIX: Added AWSGlueConsoleFullAccess + AmazonAthenaFullAccess
#      + inline GlueCatalogAndS3Access to RedshiftSpectrumRole
# ============================================================
section("6. IAM ROLES")

# GlueETLRole
try:
    iam.create_role(
        RoleName=GLUE_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"}]
        }),
        Description='Glue ETL Role'
    )
    ok(f"Created role: {GLUE_ROLE_NAME}")
except iam.exceptions.EntityAlreadyExistsException:
    ok(f"Role already exists: {GLUE_ROLE_NAME}")

for p in [
    'arn:aws:iam::aws:policy/AmazonS3FullAccess',
    'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
    'arn:aws:iam::aws:policy/AmazonRedshiftFullAccess',
    'arn:aws:iam::aws:policy/SecretsManagerReadWrite',
    'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess',
    'arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess',  # FIX: for Data API
]:
    iam.attach_role_policy(RoleName=GLUE_ROLE_NAME, PolicyArn=p)
    ok(f"  Attached: {p.split('/')[-1]}")

# RedshiftSpectrumRole
try:
    iam.create_role(
        RoleName=SPECTRUM_ROLE,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"}]
        }),
        Description='Redshift Spectrum Role'
    )
    ok(f"Created role: {SPECTRUM_ROLE}")
except iam.exceptions.EntityAlreadyExistsException:
    ok(f"Role already exists: {SPECTRUM_ROLE}")

for p in [
    'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
    'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
    'arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess',   # FIX: Glue catalog access
    'arn:aws:iam::aws:policy/AmazonAthenaFullAccess',     # FIX: Spectrum needs this
]:
    iam.attach_role_policy(RoleName=SPECTRUM_ROLE, PolicyArn=p)
    ok(f"  Attached: {p.split('/')[-1]}")

# FIX: Inline policy — explicit Glue catalog + public S3 dataset access
iam.put_role_policy(
    RoleName=SPECTRUM_ROLE,
    PolicyName='GlueCatalogAndS3Access',
    PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:CreateDatabase", "glue:DeleteDatabase",
                    "glue:GetDatabase",    "glue:GetDatabases",
                    "glue:UpdateDatabase", "glue:CreateTable",
                    "glue:DeleteTable",    "glue:UpdateTable",
                    "glue:GetTable",       "glue:GetTables",
                    "glue:BatchCreatePartition", "glue:CreatePartition",
                    "glue:DeletePartition",      "glue:UpdatePartition",
                    "glue:GetPartition",         "glue:GetPartitions",
                    "glue:BatchGetPartition"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{SCRIPTS_BUCKET}",
                    f"arn:aws:s3:::{SCRIPTS_BUCKET}/*"
                ]
            }
        ]
    })
)
ok("Inline policy added: GlueCatalogAndS3Access")

# ============================================================
# 7. REDSHIFT SERVERLESS
# FIX: publiclyAccessible=True set at creation time
#      (cannot reliably be set after creation via CLI)
# ============================================================
section("7. REDSHIFT SERVERLESS")

try:
    rs.create_namespace(
        namespaceName=NAMESPACE_NAME,
        adminUsername=DB_USER,
        adminUserPassword=DB_PASSWORD,
        dbName=DB_NAME,
        iamRoles=[SPECTRUM_ROLE_ARN],
    )
    ok(f"Namespace created: {NAMESPACE_NAME}")
except rs.exceptions.ConflictException:
    ok(f"Namespace already exists: {NAMESPACE_NAME}")

info("Waiting 15s for namespace to initialize...")
time.sleep(15)

try:
    rs.create_workgroup(
        workgroupName=WORKGROUP_NAME,
        namespaceName=NAMESPACE_NAME,
        baseCapacity=8,
        publiclyAccessible=True,    # FIX: set at creation — avoids update issues
        securityGroupIds=[sg_id],
        subnetIds=[subnet1_id, subnet2_id],
    )
    ok(f"Workgroup created: {WORKGROUP_NAME} (publiclyAccessible=True)")
except rs.exceptions.ConflictException:
    ok(f"Workgroup already exists: {WORKGROUP_NAME}")

info("Waiting for workgroup AVAILABLE (3-5 mins)...")
while True:
    try:
        wg     = rs.get_workgroup(workgroupName=WORKGROUP_NAME)['workgroup']
        status = wg['status']
        if status == 'AVAILABLE':
            ok(f"Workgroup AVAILABLE | publiclyAccessible: {wg.get('publiclyAccessible')}")
            break
        info(f"Status: {status}... waiting 15s")
        time.sleep(15)
    except Exception as e:
        info(f"Waiting... ({e})")
        time.sleep(15)

REDSHIFT_HOST = rs.get_workgroup(
    workgroupName=WORKGROUP_NAME
)['workgroup']['endpoint']['address']
ok(f"Endpoint: {REDSHIFT_HOST}")

# ============================================================
# 8. SECRETS MANAGER
# FIX: Host populated automatically after workgroup creation
#      (no manual update needed)
# ============================================================
section("8. SECRETS MANAGER")

secret_val = json.dumps({
    "host":     REDSHIFT_HOST,
    "port":     "5439",
    "dbname":   DB_NAME,
    "username": DB_USER,
    "password": DB_PASSWORD
})
try:
    sm.create_secret(Name=SECRET_NAME, SecretString=secret_val,
        Description='Redshift Serverless credentials')
    ok(f"Secret created: {SECRET_NAME}")
except sm.exceptions.ResourceExistsException:
    sm.update_secret(SecretId=SECRET_NAME, SecretString=secret_val)
    ok(f"Secret updated: {SECRET_NAME}")

# ============================================================
# 9. S3 BUCKETS + SAMPLE DATA
# FIX: amazon-reviews-pds is no longer publicly accessible
#      so we generate our own sample data and upload as parquet
# ============================================================
section("9. S3 BUCKETS + SAMPLE DATA")

for bucket_name in [SCRIPTS_BUCKET, PROCESSED_BUCKET]:
    try:
        if REGION == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': REGION})
        ok(f"Bucket created: {bucket_name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        ok(f"Bucket already exists: {bucket_name}")
    except Exception as e:
        print(f"  ❌ {e}")

# Generate sample data and upload as partitioned parquet
info("Generating sample data and uploading as parquet...")
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import random
    from datetime import date, timedelta

    categories = [
        'Apparel','Automotive','Baby','Beauty','Books',
        'Camera','Grocery','Furniture','Watches','Lawn_and_Garden'
    ]
    rows      = []
    base_date = date(2015, 1, 1)

    for i in range(2000):
        cat      = random.choice(categories)
        rev_date = base_date + timedelta(days=random.randint(0, 2920))
        rows.append({
            'marketplace':       random.choice(['US','UK','DE','JP','FR']),
            'customer_id':       f'CUST{random.randint(10000,99999)}',
            'review_id':         f'R{random.randint(1000000,9999999)}',
            'product_id':        f'B{random.randint(10000000,99999999):08d}',
            'product_parent':    f'{random.randint(100000,999999)}',
            'product_title':     f'Sample Product {i}',
            'star_rating':       random.randint(1, 5),
            'helpful_votes':     random.randint(0, 100),
            'total_votes':       random.randint(0, 150),
            'vine':              random.choice(['Y','N']),
            'verified_purchase': random.choice(['Y','N']),
            'review_headline':   f'Review headline {i}',
            'review_body':       f'Sample review body for product {i}.',
            'review_date':       rev_date.isoformat(),
            'year':              rev_date.year,
            'product_category':  cat,
        })

    df = pd.DataFrame(rows)
    ok(f"Generated {len(df)} sample rows")

    for category in categories:
        cat_df = df[df['product_category'] == category].drop(
            columns=['product_category']
        )
        buf   = io.BytesIO()
        table = pa.Table.from_pandas(cat_df, preserve_index=False)
        pq.write_table(table, buf)
        buf.seek(0)
        key = f'reviews/parquet/product_category={category}/data.parquet'
        s3.put_object(Bucket=SCRIPTS_BUCKET, Key=key, Body=buf.read())
        ok(f"  Uploaded: {key} ({len(cat_df)} rows)")

except ImportError:
    print("  ❌ Missing libraries — run: pip install pandas pyarrow")
    print("  Then re-run this script from Step 9 onwards")
    exit(1)

# ============================================================
# 10. SQL FILES
# FIX: Removed IF NOT EXISTS from CREATE EXTERNAL SCHEMA,
#      CREATE EXTERNAL TABLE, ALTER TABLE ADD partition
#      (Redshift Spectrum does not support IF NOT EXISTS on these)
# FIX: Points to our own S3 bucket instead of amazon-reviews-pds
# FIX: IAM role ARN injected dynamically (no hardcoded ARNs)
# FIX: review_date stored as varchar (avoids date parse errors)
# ============================================================
section("10. SQL FILES")

reviewsschema_sql = f"""CREATE EXTERNAL SCHEMA amzreviews
FROM DATA CATALOG
DATABASE 'amzreviews'
IAM_ROLE '{SPECTRUM_ROLE_ARN}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE amzreviews.reviews(
  marketplace varchar(10),
  customer_id varchar(15),
  review_id varchar(15),
  product_id varchar(25),
  product_parent varchar(15),
  product_title varchar(100),
  star_rating int,
  helpful_votes int,
  total_votes int,
  vine varchar(5),
  verified_purchase varchar(5),
  review_headline varchar(200),
  review_body varchar(2048),
  review_date varchar(20),
  year int)
PARTITIONED BY (product_category varchar(25))
STORED AS PARQUET
LOCATION 's3://{SCRIPTS_BUCKET}/reviews/parquet/';

ALTER TABLE amzreviews.reviews ADD
  partition(product_category='Apparel')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Apparel/'
  partition(product_category='Automotive')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Automotive/'
  partition(product_category='Baby')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Baby/'
  partition(product_category='Beauty')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Beauty/'
  partition(product_category='Books')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Books/'
  partition(product_category='Camera')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Camera/'
  partition(product_category='Grocery')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Grocery/'
  partition(product_category='Furniture')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Furniture/'
  partition(product_category='Watches')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Watches/'
  partition(product_category='Lawn_and_Garden')
  location 's3://{SCRIPTS_BUCKET}/reviews/parquet/product_category=Lawn_and_Garden/';

CREATE TABLE public.reviews(
  marketplace varchar(10),
  customer_id varchar(15),
  review_id varchar(15),
  product_id varchar(25) DISTKEY,
  product_parent varchar(15),
  product_title varchar(100),
  star_rating int,
  helpful_votes int,
  total_votes int,
  vine varchar(5),
  verified_purchase varchar(5),
  review_headline varchar(200),
  review_body varchar(2048),
  review_date varchar(20),
  year int,
  product_category varchar(25))
SORTKEY (year)
"""

etl_sql = """TRUNCATE TABLE public.reviews;

INSERT INTO public.reviews
SELECT
  marketplace, customer_id, review_id, product_id,
  product_parent, product_title, star_rating, helpful_votes,
  total_votes, vine, verified_purchase, review_headline,
  review_body, review_date, year, product_category
FROM amzreviews.reviews
WHERE year >= 2015
"""

topreviews_sql = f"""UNLOAD (
  'SELECT
     product_category,
     product_title,
     AVG(CAST(star_rating AS FLOAT)) AS avg_rating,
     SUM(helpful_votes) AS total_helpful_votes,
     COUNT(*) AS review_count
   FROM public.reviews
   GROUP BY product_category, product_title
   ORDER BY avg_rating DESC'
)
TO 's3://{PROCESSED_BUCKET}/output/topreviews_'
IAM_ROLE '{SPECTRUM_ROLE_ARN}'
FORMAT AS PARQUET
ALLOWOVERWRITE
"""

for filename, content in [
    ('sql/reviewsschema.sql', reviewsschema_sql),
    ('sql/etl.sql',           etl_sql),
    ('sql/topreviews.sql',    topreviews_sql),
]:
    s3.put_object(Bucket=SCRIPTS_BUCKET, Key=filename,
        Body=content.encode('utf-8'))
    ok(f"Uploaded: {filename}")

# ============================================================
# 11. rs_query.py — Redshift Data API version
# FIX: Replaced psycopg2 with Redshift Data API
#      - psycopg2 needs VPC connectivity (port 5439) from Glue
#      - Data API uses AWS API calls — no VPC port issues
#      - DDL statements (CREATE EXTERNAL SCHEMA) persist reliably
# FIX: Pre-creates Glue Data Catalog database before SQL runs
#      (CREATE EXTERNAL DATABASE IF NOT EXISTS is unreliable)
# FIX: Verifies schema using SVV_EXTERNAL_SCHEMAS after creation
# ============================================================
section("11. rs_query.py")

rs_query_script = f'''import sys
import boto3
import json
import time

REGION         = 'us-east-1'
WORKGROUP_NAME = '{WORKGROUP_NAME}'
DATABASE       = '{DB_NAME}'

def get_secret(secret_name, region_name):
    client = boto3.client('secretsmanager', region_name=region_name)
    resp   = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in resp:
        return json.loads(resp['SecretString'])
    import base64
    return json.loads(base64.b64decode(resp['SecretBinary']))

def wait_for_statement(client, statement_id, label=''):
    while True:
        resp   = client.describe_statement(Id=statement_id)
        status = resp['Status']
        if status == 'FINISHED':
            print(f"  ✅ {{label}} SUCCESS")
            return True
        elif status in ('FAILED', 'ABORTED'):
            error = resp.get('Error', 'Unknown error')
            raise Exception(f"  ❌ {{label}} FAILED: {{error}}")
        else:
            print(f"  ⏳ {{label}} {{status}}... waiting 5s")
            time.sleep(5)

def run_statement(rd, secret_arn, sql, label):
    resp = rd.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=DATABASE,
        SecretArn=secret_arn,
        Sql=sql,
        StatementName=label[:64]
    )
    wait_for_statement(rd, resp['Id'], label)
    return resp['Id']

def ensure_glue_database(db_name, region_name):
    glue = boto3.client('glue', region_name=region_name)
    try:
        glue.get_database(Name=db_name)
        print(f"  ✅ Glue DB '{{db_name}}' already exists")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={{'Name': db_name,
            'Description': 'External DB for Redshift Spectrum'}})
        print(f"  ✅ Glue DB '{{db_name}}' created")
    except Exception as e:
        print(f"  ⚠️  Glue DB: {{e}}")

def run_sql_from_s3(s3_sql_file, secret_name, region_name):
    s3c    = boto3.client('s3', region_name=region_name)
    bucket = s3_sql_file.replace("s3://", "").split("/")[0]
    key    = "/".join(s3_sql_file.replace("s3://", "").split("/")[1:])
    print(f"Reading SQL from: s3://{{bucket}}/{{key}}")
    sql    = s3c.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    sm         = boto3.client("secretsmanager", region_name=region_name)
    secret_arn = sm.describe_secret(SecretId=secret_name)["ARN"]
    rd         = boto3.client("redshift-data", region_name=region_name)

    # Pre-create Glue DB before reviewsschema.sql
    if "reviewsschema" in s3_sql_file.lower():
        print("\\n--- Pre-creating Glue Data Catalog database ---")
        ensure_glue_database("amzreviews", region_name)

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    print(f"\\nTotal statements: {{len(statements)}}\\n")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\\n", " ")
        print(f"--- Statement {{i}}/{{len(statements)}} ---")
        print(f"SQL: {{preview}}...")
        try:
            run_statement(rd, secret_arn, stmt, f"stmt-{{i}}")
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["already exists", "duplicate"]):
                print(f"  ⚠️  Already exists — skipping")
            else:
                raise

    # Verify schema after reviewsschema.sql
    if "reviewsschema" in s3_sql_file.lower():
        print("\\n--- Verifying schema 'amzreviews' ---")
        stmt_id = run_statement(rd, secret_arn,
            "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS WHERE schemaname='amzreviews'",
            "verify-schema")
        result  = rd.get_statement_result(Id=stmt_id)
        rows    = result.get("Records", [])
        if rows:
            print("  ✅ Schema 'amzreviews' confirmed!")
        else:
            raise Exception("Schema 'amzreviews' NOT found — check IAM role!")

    # Show row count after etl.sql
    if "etl.sql" in s3_sql_file.lower():
        print("\\n--- Row count check ---")
        stmt_id = run_statement(rd, secret_arn,
            "SELECT COUNT(*) FROM public.reviews", "count-check")
        result  = rd.get_statement_result(Id=stmt_id)
        count   = result["Records"][0][0].get("longValue", 0)
        print(f"  ✅ Rows in public.reviews: {{count}}")

    print("\\n✅ All statements completed successfully!")

# Parse args
args = {{}}
argv = sys.argv[1:]
i = 0
while i < len(argv):
    if argv[i].startswith("--") and i + 1 < len(argv):
        args[argv[i][2:]] = argv[i+1]
        i += 2
    else:
        i += 1

secret_name = args.get("secret_name", "redshift/etl-credentials")
region_name = args.get("region_name", "us-east-1")
s3_sql_file = args.get("s3_sql_file", "")

print(f"secret_name : {{secret_name}}")
print(f"region_name : {{region_name}}")
print(f"s3_sql_file : {{s3_sql_file}}")
print()

run_sql_from_s3(s3_sql_file, secret_name, region_name)
'''

s3.put_object(
    Bucket=SCRIPTS_BUCKET,
    Key='python/rs_query.py',
    Body=rs_query_script.encode('utf-8')
)
ok(f"Uploaded: python/rs_query.py")

# ============================================================
# 12. GLUE JOB
# FIX: No connections attached (connections cause timeout in Python Shell)
# FIX: No --extra-py-files or .egg references
# FIX: --additional-python-modules not needed (Data API uses boto3 only)
# ============================================================
section("12. GLUE JOB")

job_config = {
    'Name': GLUE_JOB_NAME,
    'Role': GLUE_ROLE_ARN,
    'Command': {
        'Name': 'pythonshell',
        'ScriptLocation': f's3://{SCRIPTS_BUCKET}/python/rs_query.py',
        'PythonVersion': '3.9'
    },
    'DefaultArguments': {
        '--secret_name': SECRET_NAME,
        '--region_name': REGION,
        '--s3_sql_file': f's3://{SCRIPTS_BUCKET}/sql/reviewsschema.sql',
    },
    'Connections': {'Connections': []},   # FIX: no connections
    'ExecutionProperty': {'MaxConcurrentRuns': 1},
    'GlueVersion': '3.0',
    'Description': 'ETL job using Redshift Data API',
}

try:
    glue.create_job(**job_config)
    ok(f"Glue job created: {GLUE_JOB_NAME}")
except glue.exceptions.AlreadyExistsException:
    config_copy = {k: v for k, v in job_config.items() if k != 'Name'}
    glue.update_job(JobName=GLUE_JOB_NAME, JobUpdate=config_copy)
    ok(f"Glue job updated: {GLUE_JOB_NAME}")

# ============================================================
# DONE
# ============================================================
print(f"""
{'='*60}
  🎉 SETUP COMPLETE — ALL RESOURCES CREATED

  Account ID     : {ACCOUNT_ID}
  Region         : {REGION}
  VPC ID         : {vpc_id}
  Subnet 1       : {subnet1_id} ({AZ1})
  Subnet 2       : {subnet2_id} ({AZ2})
  Security Group : {sg_id}
  Redshift Host  : {REDSHIFT_HOST}
  Scripts Bucket : s3://{SCRIPTS_BUCKET}
  Output Bucket  : s3://{PROCESSED_BUCKET}
  Glue Job       : {GLUE_JOB_NAME}
  Secret         : {SECRET_NAME}

  NEXT STEPS — Run Glue jobs in this order:
  1. Job param --s3_sql_file = sql/reviewsschema.sql → Run
  2. Job param --s3_sql_file = sql/etl.sql           → Run
  3. Job param --s3_sql_file = sql/topreviews.sql    → Run
  4. Check output: s3://{PROCESSED_BUCKET}/output/
{'='*60}
""")