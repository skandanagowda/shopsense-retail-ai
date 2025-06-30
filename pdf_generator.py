import fitz  # PyMuPDF
import boto3
import json
from io import BytesIO
import urllib.parse
import textwrap
import re

s3 = boto3.client('s3')

def generate_pdf(llm_json_data, report_type, report_date):
    pdf_buffer = BytesIO()
    doc = fitz.open()
    page = doc.new_page()
    y = 50
    subheading_mode = False

    def wrap_text(text, bullet=False):
        wrapper = textwrap.TextWrapper(width=90, break_long_words=True)
        lines = wrapper.wrap(text)
        wrapped = []
        for i, line in enumerate(lines):
            if bullet and i == 0:
                wrapped.append(u"\u2022 " + line)
            elif bullet:
                wrapped.append("    " + line)
            else:
                wrapped.append(line)
        return wrapped

    # Title
    title = f"ShopSense {report_type.title()} Report · {report_date}"
    page.insert_text((50, y), title, fontsize=16)
    y += 30

    lines = llm_json_data.get("llm_summary", "").split('\n')

    for raw in lines:
        line = raw.strip()
        if not line:
            y += 10
            subheading_mode = False
            continue

        if y > 720:
            page = doc.new_page()
            y = 50

        # Remove markdown artifacts but only after checking for subheadings
        raw_clean = re.sub(r"[·\-\s]+", "", line)

        # Main headings (e.g. Summary, Performance Report)
        if "Summary" in line or "Performance Report" in line:
            page.insert_text((50, y), line.strip("#·- "), fontsize=13)
            y += 20
            subheading_mode = False
            continue

        # Subheadings like · · Sales Highlights
        if re.match(r"^[·\-\s]{0,3}(Sales Highlights|Consumer Behavior|External Influences|Strategic Recommendations)", line):
            clean_heading = re.sub(r"^[#·\-\s]+", "", line).rstrip(":")
            page.insert_text((55, y), clean_heading, fontsize=11)
            y += 16
            subheading_mode = True
            continue

        # Now clean markdown for all remaining lines
        line = re.sub(r"^#+\s*", "", line)
        line = re.sub(r"^[·\-\s]+", "", line)
        line = re.sub(r"`(.*?)`", r"\1", line)
        line = re.sub(r"\*\*(.*?)\*\*", r"\1", line)

        # Main headings (e.g. Summary, Performance Report)
        if "Summary" in line or "Performance Report" in line:
            page.insert_text((50, y), line.strip(), fontsize=13)
            y += 20
            subheading_mode = False
            continue

        # Subheading (e.g. Sales Highlights, Strategic Recommendations)
        if line.endswith(":") or line in [
            "Sales Highlights", "Consumer Behavior", "External Influences", "Strategic Recommendations"
        ]:
            page.insert_text((55, y), line.rstrip(":"), fontsize=11)
            y += 16
            subheading_mode = True
            continue

        # Bullet content
        wrapped = wrap_text(line, bullet=True)
        for wline in wrapped:
            indent = 85 if subheading_mode else 65
            page.insert_text((indent, y), wline, fontsize=10)
            y += 12

    doc.save(pdf_buffer)
    pdf_buffer.seek(0)
    return pdf_buffer


def lambda_handler(event, context):
    try:
        # Get S3 key and bucket from event trigger
        record = event['Records'][0]['s3']
        bucket = record['bucket']['name']
        json_key = urllib.parse.unquote_plus(record['object']['key'])

        print(f"Generating PDF for: s3://{bucket}/{json_key}")

        # Extract type and date from key: llm-insights/{weekly|monthly}/report_YYYY-MM[-DD].json
        parts = json_key.split("/")
        report_type = parts[1]
        report_date = parts[2].replace("report_", "").replace(".json", "")

        # Read JSON from S3
        llm_json_data = json.loads(s3.get_object(Bucket=bucket, Key=json_key)['Body'].read().decode('utf-8'))

        # Generate PDF
        pdf_buffer = generate_pdf(llm_json_data, report_type, report_date)

        # Save PDF to S3
        pdf_key = f"pdf-reports/{report_type}/ShopSense_{report_type.title()}_{report_date}.pdf"
        s3.put_object(Bucket=bucket, Key=pdf_key, Body=pdf_buffer.getvalue(), ContentType='application/pdf')

        print(f"PDF generated at: s3://{bucket}/{pdf_key}")

        return {
            "statusCode": 200,
            "pdf_key": pdf_key
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "error": str(e)
        }
