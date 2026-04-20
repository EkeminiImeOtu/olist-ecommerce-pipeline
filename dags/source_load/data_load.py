import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import boto3

ssm = boto3.client('ssm', region_name='us-east-1')
s3  = boto3.client('s3',  region_name='us-east-1')

BUCKET = 'olist-data-010659611291'
SCHEMA = 'OLIST_RAW'

def get_param(name):
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def get_connection():
    conn = snow.connect(
        user=get_param('/snowflake/username'),
        password=get_param('/snowflake/password'),
        account=get_param('/snowflake/accountname'),
        warehouse='COMPUTE_WH',
        database='PROD',
        schema=SCHEMA
    )
    cur = conn.cursor()
    cur.execute(f"USE DATABASE PROD")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    print(f"Connected to Snowflake — PROD.{SCHEMA}")
    return conn, cur

def load_table(conn, cur, filename, table_name):
    print(f"Loading {filename} → {table_name}...")
    obj = s3.get_object(Bucket=BUCKET, Key=f'raw_files/{filename}')
    df = pd.read_csv(obj['Body'])
    df.columns = [c.upper() for c in df.columns]
    cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
    write_pandas(conn, df, table_name, auto_create_table=True, overwrite=True)
    print(f"  ✅ {len(df):,} rows loaded into {table_name}")
    return len(df)

def run_script():
    conn, cur = get_connection()
    tables = [
        ("olist_orders_dataset.csv",              "ORDERS_RAW"),
        ("olist_customers_dataset.csv",           "CUSTOMERS_RAW"),
        ("olist_order_items_dataset.csv",         "ORDER_ITEMS_RAW"),
        ("olist_order_payments_dataset.csv",      "ORDER_PAYMENTS_RAW"),
        ("olist_order_reviews_dataset.csv",       "ORDER_REVIEWS_RAW"),
        ("olist_products_dataset.csv",            "PRODUCTS_RAW"),
        ("olist_sellers_dataset.csv",             "SELLERS_RAW"),
        ("olist_geolocation_dataset.csv",         "GEOLOCATION_RAW"),
        ("product_category_name_translation.csv", "CATEGORY_TRANSLATION_RAW"),
    ]
    total_rows = 0
    for filename, table_name in tables:
        total_rows += load_table(conn, cur, filename, table_name)
    cur.close()
    conn.close()
    print(f"\n🎉 Done! Total rows loaded: {total_rows:,}")

if __name__ == "__main__":
    run_script()
