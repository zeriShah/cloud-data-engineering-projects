import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import io
from datetime import date, timedelta

REGION = 'us-east-1'
SCRIPTS_BUCKET = 'redshift-scripts-data-us'
s3 = boto3.client('s3', region_name=REGION)

print("Generating sample data...")
categories = [
    'Apparel','Automotive','Baby','Beauty','Books',
    'Camera','Grocery','Furniture','Watches','Lawn_and_Garden'
]
rows = []
base_date = date(2015, 1, 1)

for i in range(2000):
    cat = random.choice(categories)
    rev_date = base_date + timedelta(days=random.randint(0, 2920))
    rows.append({
        'marketplace': random.choice(['US','UK','DE','JP','FR']),
        'customer_id': f'CUST{random.randint(10000,99999)}',
        'review_id': f'R{random.randint(1000000,9999999)}',
        'product_id': f'B{random.randint(10000000,99999999):08d}',
        'product_parent': f'{random.randint(100000,999999)}',
        'product_title': f'Sample Product {i}',
        'star_rating': random.randint(1, 5),
        'helpful_votes': random.randint(0, 100),
        'total_votes': random.randint(0, 150),
        'vine': random.choice(['Y','N']),
        'verified_purchase': random.choice(['Y','N']),
        'review_headline': f'Review headline {i}',
        'review_body': f'Sample review body for product {i}.',
        'review_date': rev_date.isoformat(),
        'year': rev_date.year,
        'product_category': cat,
    })

df = pd.DataFrame(rows)

# Fix data types to match Redshift schema (int32 instead of int64)
df['star_rating'] = df['star_rating'].astype('int32')
df['helpful_votes'] = df['helpful_votes'].astype('int32')
df['total_votes'] = df['total_votes'].astype('int32')
df['year'] = df['year'].astype('int32')

print(f"[OK] Generated {len(df)} sample rows")

for category in categories:
    cat_df = df[df['product_category'] == category].drop(columns=['product_category'])
    buf = io.BytesIO()
    table = pa.Table.from_pandas(cat_df, preserve_index=False)
    pq.write_table(table, buf)
    buf.seek(0)
    key = f'reviews/parquet/product_category={category}/data.parquet'
    s3.put_object(Bucket=SCRIPTS_BUCKET, Key=key, Body=buf.read())
    print(f"[OK] Uploaded: {key} ({len(cat_df)} rows)")

print("\n[OK] All sample data uploaded successfully!")
