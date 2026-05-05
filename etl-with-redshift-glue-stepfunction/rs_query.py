import sys
import boto3
import json
import time

REGION = 'us-east-1'
WORKGROUP_NAME = 'redshift-workgroup'
DATABASE = 'dev'

def get_secret(secret_name, region_name):
    client = boto3.client('secretsmanager', region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in resp:
        return json.loads(resp['SecretString'])
    import base64
    return json.loads(base64.b64decode(resp['SecretBinary']))

def wait_for_statement(client, statement_id, label=''):
    while True:
        resp = client.describe_statement(Id=statement_id)
        status = resp['Status']
        if status == 'FINISHED':
            print(f"  [OK] {label} SUCCESS")
            return True
        elif status in ('FAILED', 'ABORTED'):
            error = resp.get('Error', 'Unknown error')
            raise Exception(f"  [ERROR] {label} FAILED: {error}")
        else:
            print(f"  [WAIT] {label} {status}... waiting 5s")
            time.sleep(5)

def run_statement(rd, secret_arn, sql, label):
    # Try Redshift Serverless first (WorkgroupName), fallback to provisioned (ClusterIdentifier)
    try:
        resp = rd.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE,
            SecretArn=secret_arn,
            Sql=sql,
            StatementName=label[:64]
        )
    except Exception as e:
        if 'WorkgroupName' in str(e):
            # Older boto3 - upgrade required
            print(f"  [WARN] boto3 version too old, needs upgrade for Serverless support")
            raise Exception("Please add --additional-python-modules boto3==1.26.0 to Glue job")
        raise
    wait_for_statement(rd, resp['Id'], label)
    return resp['Id']

def ensure_glue_database(db_name, region_name):
    glue = boto3.client('glue', region_name=region_name)
    try:
        glue.get_database(Name=db_name)
        print(f"  [OK] Glue DB '{db_name}' already exists")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={'Name': db_name,
            'Description': 'External DB for Redshift Spectrum'})
        print(f"  [OK] Glue DB '{db_name}' created")
    except Exception as e:
        print(f"  [WARN] Glue DB: {e}")

def run_sql_from_s3(s3_sql_file, secret_name, region_name):
    s3c = boto3.client('s3', region_name=region_name)
    bucket = s3_sql_file.replace("s3://", "").split("/")[0]
    key = "/".join(s3_sql_file.replace("s3://", "").split("/")[1:])
    print(f"Reading SQL from: s3://{bucket}/{key}")
    sql = s3c.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    sm = boto3.client("secretsmanager", region_name=region_name)
    secret_arn = sm.describe_secret(SecretId=secret_name)["ARN"]
    rd = boto3.client("redshift-data", region_name=region_name)

    # Pre-create Glue DB before reviewsschema.sql
    if "reviewsschema" in s3_sql_file.lower():
        print("\n--- Pre-creating Glue Data Catalog database ---")
        ensure_glue_database("amzreviews", region_name)

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    print(f"\nTotal statements: {len(statements)}\n")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        print(f"--- Statement {i}/{len(statements)} ---")
        print(f"SQL: {preview}...")
        try:
            run_statement(rd, secret_arn, stmt, f"stmt-{i}")
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["already exists", "duplicate"]):
                print(f"  [WARN] Already exists - skipping")
            else:
                raise

    # Verify schema after reviewsschema.sql
    if "reviewsschema" in s3_sql_file.lower():
        print("\n--- Verifying schema 'amzreviews' ---")
        stmt_id = run_statement(rd, secret_arn,
            "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS WHERE schemaname='amzreviews'",
            "verify-schema")
        result = rd.get_statement_result(Id=stmt_id)
        rows = result.get("Records", [])
        if rows:
            print("  [OK] Schema 'amzreviews' confirmed!")
        else:
            raise Exception("Schema 'amzreviews' NOT found - check IAM role!")

    # Show row count after etl.sql
    if "etl.sql" in s3_sql_file.lower():
        print("\n--- Row count check ---")
        stmt_id = run_statement(rd, secret_arn,
            "SELECT COUNT(*) FROM public.reviews", "count-check")
        result = rd.get_statement_result(Id=stmt_id)
        count = result["Records"][0][0].get("longValue", 0)
        print(f"  [OK] Rows in public.reviews: {count}")

    print("\n[OK] All statements completed successfully!")

# Parse args
args = {}
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

print(f"secret_name : {secret_name}")
print(f"region_name : {region_name}")
print(f"s3_sql_file : {s3_sql_file}")
print()

run_sql_from_s3(s3_sql_file, secret_name, region_name)
