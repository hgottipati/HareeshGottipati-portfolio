import boto3
import pandas as pd

s3 = boto3.client('s3')
bucket = 'bookings-data'
file_key = 'raw/bookings.csv'

def extract_data():
    obj = s3.get_object(Bucket=bucket, Key=file_key)
    df = pd.read_csv(obj['Body'])
    return df

def transform_data(df):
    df['tax_amount'] = df['price'] * 0.1  # Simplified tax calc
    return df

def load_data(df):
    df.to_parquet(f's3://{bucket}/processed/bookings.parquet')

if __name__ == "__main__":
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)