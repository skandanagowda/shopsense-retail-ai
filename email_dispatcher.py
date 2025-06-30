import boto3
import os
import json
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")
ses = boto3.client("ses")

BUCKET_NAME = "sk-shopsense-retail-uploads"
REPORT_PREFIXES = ["pdf-reports/weekly/", "pdf-reports/monthly/"]
SES_SENDER = os.environ["SES_SENDER"]
SES_RECIPIENT = os.environ["SES_RECIPIENT"]
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "60"))

def get_recent_pdfs():
    recent_keys = []
    now = datetime.now(timezone.utc)
    cutoff = datetime(2024, 4, 24, tzinfo=timezone.utc)

    for prefix in REPORT_PREFIXES:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]
                # Accept all files, skip time filter
                if key.endswith(".pdf"):
                    url = s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": BUCKET_NAME, "Key": key},
                        ExpiresIn=3600
                    )
                    recent_keys.append((key, url))

    return recent_keys

def send_email(pdfs):
    if not pdfs:
        print("No recent PDFs found, skipping email.")
        return

    subject = "ShopSense Report Digest"
    body = "Hi,\n\nHere are the latest ShopSense reports:\n\n"

    for key, url in pdfs:
        report_type = "Weekly" if "weekly" in key else "Monthly"
        date_str = key.split("_")[-1].replace(".pdf", "")
        body += f"📄 {report_type} Report – {date_str}:\n{url}\n\n"

    body += "Best,\nShopSense Bot"

    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body}},
        },
    )

def lambda_handler(event, context):
    try:
        pdfs = get_recent_pdfs()
        send_email(pdfs)
        return {
            "statusCode": 200,
            "message": f"Sent summary email with {len(pdfs)} PDF(s)."
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "error": str(e)}
