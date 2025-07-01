import json
import boto3
import pandas as pd
import numpy as np
from dateutil.parser import parse
from io import BytesIO
import os
from datetime import datetime

s3 = boto3.client("s3")

def lambda_handler(event, context):
    print("Lambda triggered with event:", json.dumps(event))

    try:
        # Step 1: Extract bucket and key
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"File received: Bucket = {bucket}, Key = {key}")

        # Step 2: Read the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()

        try:
            df = pd.read_csv(BytesIO(file_content))
            print("Raw preview:\n", df.head())
        except Exception as read_err:
            print("Failed to read CSV:", str(read_err))
            raise Exception("Invalid or unreadable CSV format")

        # Step 3: Standardize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        # Check if the column 'dt' exists and rename it to 'date'
        if 'dt' in df.columns:
            print("Found 'dt' column, renaming to 'date'.")
            df.rename(columns={'dt': 'date'}, inplace=True)

        # Step 4: Robust date parsing
        if 'date' in df.columns:
            def try_parse_date(x):
                try:
                    return parse(str(x))
                except:
                    return pd.NaT
            df['date'] = df['date'].apply(try_parse_date)
            df['date'].fillna(method='ffill', inplace=True)

        # Step 5: Convert numeric columns
        if 'sale_amount' in df.columns:
            df['sale_amount'] = pd.to_numeric(df['sale_amount'], errors='coerce')
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

        # Step 6: Outlier handling
        num_cols = df.select_dtypes(include='number').columns
        outlier_mask = {}
        for col in num_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            is_outlier = (df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)
            outlier_mask[col] = is_outlier
            df.loc[is_outlier, col] = np.nan

        # Step 7: Fill missing values (updated with .loc to avoid chained assignment)
        for col in num_cols:
            outliers = outlier_mask[col]
            median_val = df[col].median()
            df.loc[outliers, col] = median_val
            df[col] = df[col].fillna(df[col].mean()).round(2)

        # Step 8: String cleaning
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", "unknown")
            df[col].fillna("unknown", inplace=True)
            df[col] = df[col].str.lower()

        if 'city' in df.columns:
            df['city'] = df['city'].str.title()

        # Drop duplicates
        df.drop_duplicates(inplace=True)
        drop_cols = ['hours_sale', 'hours_stock_status']
        df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

        print("Cleaned preview:\n", df.head())

        # Step 9: Save to buffer
        cleaned_buffer = BytesIO()
        df.to_csv(cleaned_buffer, index=False)
        cleaned_buffer.seek(0)

        # Step 10: Upload to partitioned & timestamped S3 path (one file per date)
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename_base = os.path.basename(key).replace("raw_", "").lower()

        for dt_val, sub_df in df.groupby('date'):
            partition_value = dt_val.strftime('%Y-%m-%d')
            cleaned_buffer = BytesIO()
            sub_df.to_csv(cleaned_buffer, index=False)
            cleaned_buffer.seek(0)

            new_filename = f"cleaned_{timestamp}_{partition_value}_{filename_base}"
            cleaned_key = f"retail-cleaned-data/dt={partition_value}/{new_filename}"

            response = s3.put_object(
                Bucket=bucket,
                Key=cleaned_key,
                Body=cleaned_buffer.getvalue()
            )
            print(f"Uploaded partition {partition_value} → s3://{bucket}/{cleaned_key}")

        return {
            'statusCode': 200,
            'body': json.dumps('Cleaning complete and partitioned upload successful!')
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
