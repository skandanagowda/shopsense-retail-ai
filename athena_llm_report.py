import os
import boto3
import time
from openai import OpenAI
from datetime import datetime, timedelta
from io import StringIO
import csv
import json

# --- CLIENTS ---
s3 = boto3.client("s3")
athena = boto3.client("athena")
client = OpenAI(
    base_url=os.environ["OPENROUTER_BASE_URL"],
    api_key=os.environ["OPENROUTER_API_KEY"]
)

# --- Core logic moved into this new helper function ---
def generate_and_save_report(database, table, output_location, start_date_str, end_date_str, report_date_str, report_mode):
    """
    Generates and saves a single report for a specific time period.
    """
    print(f" Generating report for period: {start_date_str} -> {end_date_str}")

    date_filter = f"""
        TRY(CAST(DATE_PARSE(dt, '%Y-%m-%d') AS DATE)) IS NOT NULL
        AND DATE_PARSE(dt, '%Y-%m-%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
    """

    queries = {
        "top_sellers": f"""
            SELECT product_id, SUM(sale_amount) AS total_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY product_id
            ORDER BY total_sales DESC
            LIMIT 10;
        """,
       "holiday_sales": f"""
            SELECT 
                CASE 
                    WHEN coalesce(holiday_flag, 0) >= 0.9 THEN 'Holiday'
                    ELSE 'Non-Holiday' 
                END AS day_type,
                ROUND(AVG(sale_amount), 2) AS avg_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY 
                CASE 
                    WHEN coalesce(holiday_flag, 0) >= 0.9 THEN 'Holiday'
                    ELSE 'Non-Holiday' 
                END;
        """,
       "weather_impact": f"""
            SELECT 
                CASE WHEN precpt > 5 THEN 'Rainy' ELSE 'Dry' END AS weather,
                CASE 
                    WHEN avg_temperature < 15 THEN 'Cold'
                    WHEN avg_temperature BETWEEN 15 AND 30 THEN 'Moderate'
                    ELSE 'Hot' 
                END AS temp_range,
                ROUND(AVG(sale_amount), 2) AS avg_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY 1, 2;
        """,
        "weekly_trend": f"""
            SELECT 
                date_trunc('week', DATE_PARSE(dt, '%Y-%m-%d')) AS week,
                SUM(sale_amount) AS total_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY 1
            ORDER BY 1;
        """,
       "discount_impact": f"""
            SELECT 
                CASE 
                    WHEN discount = 0 THEN 'No Discount'
                    WHEN discount < 0.5 THEN 'Low Discount'
                    ELSE 'High Discount'
                END AS discount_level,
                ROUND(AVG(sale_amount), 2) AS avg_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY 1;
        """,
        "sales_by_city": f"""
            SELECT city_id, ROUND(SUM(sale_amount), 2) AS total_sales
            FROM {table}
            WHERE {date_filter}
            GROUP BY city_id
            ORDER BY total_sales DESC
            LIMIT 10;
        """,
        "co_purchase_simulation": f"""
            SELECT 
                product_id, 
                COUNT(DISTINCT CAST(store_id AS VARCHAR) || dt) AS product_days
            FROM {table}
            WHERE 
                {date_filter}
                AND sale_amount > 0
            GROUP BY product_id
            ORDER BY product_days DESC
            LIMIT 10;
        """,
    }

    # Nested helper functions for running queries and processing results
    def run_query(query):
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location}
        )
        query_id = response["QueryExecutionId"]
        final_result = None
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_id)
            state = result["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                final_result = result
                break
            time.sleep(1)
        if state != "SUCCEEDED":
            reason = final_result["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided.")
            raise Exception(f"Athena query for period {report_date_str} failed: {state}. Reason: {reason}")
        return athena.get_query_results(QueryExecutionId=query_id)

    def results_to_table_data(results):
        rows = results["ResultSet"]["Rows"]
        if not rows: return [["No Data"], [""]]
        headers = [col["VarCharValue"] for col in rows[0]["Data"]]
        table_data = [headers]
        for row in rows[1:]:
            table_data.append([col.get("VarCharValue", "") for col in row["Data"]])
        return table_data

    # Run all queries for the period
    tables = {}
    for name, query in queries.items():
        print(f"  -> Running '{name}' sub-query...")
        result = run_query(query)
        tables[name] = results_to_table_data(result)
        
    # Check if we got any data before proceeding
    if not tables.get("top_sellers") or len(tables["top_sellers"]) <= 1:
        print(f" No data found for period {report_date_str}. Skipping report generation.")
        return

    # --- LLM and S3 saving logic ---
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(tables["top_sellers"])
    actual_key = f"actual-sales/{report_mode}/actual_{report_date_str}.csv"
    s3.put_object(Bucket="sk-shopsense-retail-uploads", Key=actual_key, Body=csv_buffer.getvalue())
    print(f"  -> Saved CSV to {actual_key}")

    def markdown_block(table):
        return "\n".join(["| " + " | ".join(row) + " |" for row in table])

         # --- PROMPT STARTS HERE ---
    prompt = f"""
    You are a professional retail business analyst.

    Your task is to write a business-ready {report_mode} sales performance report based only on the structured data below.

    📌 STRICT RULES:
    1.  Do NOT invent or assume any product IDs, sales numbers, city names, or trends.
    2.  Only use the exact values, product IDs, and city names provided in the tables.
    3.  Do NOT hallucinate additional content. This is a factual report for executives.

    INPUT DATA (Structured Tables):

    Top-Selling Products:
    {markdown_block(tables['top_sellers'])}

    Holiday Sales Impact:
    {markdown_block(tables['holiday_sales'])}

    Weather Influence:
    {markdown_block(tables['weather_impact'])}

    Weekly Trends:
    {markdown_block(tables['weekly_trend'])}

    Discount Impact:
    {markdown_block(tables['discount_impact'])}

    City-wise Sales:
    {markdown_block(tables['sales_by_city'])}

    Co-purchase Simulation:
    {markdown_block(tables['co_purchase_simulation'])}

    YOUR TASK:
    Write a clear and professional report in this structure:
    - Executive Summary: 2-3 sentence overview of overall trends.
    - Sales Highlights: Bullet points based on top products, cities, trends.
    - Consumer Behavior: Bullet points from co-purchase + product preferences.
    - External Influences: Bullet points from weather, holidays, discount analysis.
    - Strategic Recommendations: Business actions derived from observed data.

    Use simple, non-technical language that’s suitable for business stakeholders. Be concise and data-driven.
        """
    # --- PROMPT ENDS HERE ---

    PREFERRED_MODELS = [
    "deepseek/deepseek-chat:free",
    "google/gemini-2.0-flash-experimental:free",
    "meta-llama/llama-3.1-8b-instruct:free"
    ]
    for model in PREFERRED_MODELS:
        try:
            summary = client.chat.completions.create(
                model=model,
                messages=[
                {"role": "system", "content": "You are a retail data analyst."},
                {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.7,
            )
            break  # If successful, stop trying others
        except Exception as e:
            print(f"Model {model} failed: {e}")

    insight = summary.choices[0].message.content
    
    llm_output = {
        "report_type": report_mode,
        "report_date": report_date_str,
        "generated_on": datetime.utcnow().isoformat() + "Z",
        "llm_summary": insight
    }
    llm_key = f"llm-insights/{report_mode}/report_{report_date_str}.json"
    s3.put_object(Bucket="sk-shopsense-retail-uploads", Key=llm_key, Body=json.dumps(llm_output, indent=2))
    print(f"  -> Saved JSON insight to {llm_key}")


def lambda_handler(event, context):
    database = os.environ["ATHENA_DATABASE"]
    table = os.environ["ATHENA_TABLE"]
    output_location = os.environ["ATHENA_OUTPUT_LOCATION"]
    report_mode = os.environ.get("REPORT_MODE", "weekly")

    # --- Function to get the full range of data ---
    def get_full_date_range():
        print("🔍 Finding the full date range of the dataset...")
        query = f"SELECT MIN(dt), MAX(dt) FROM {table} WHERE TRY_CAST(DATE_PARSE(dt, '%Y-%m-%d') AS DATE) IS NOT NULL"
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location}
        )
        query_id = response["QueryExecutionId"]
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_id)
            state = result["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)
        if state != "SUCCEEDED":
            raise Exception("Athena query to get MIN/MAX dates failed.")
            
        results = athena.get_query_results(QueryExecutionId=query_id)
        row = results["ResultSet"]["Rows"][1]["Data"]
        min_date_str = row[0].get("VarCharValue")
        max_date_str = row[1].get("VarCharValue")
        
        if not min_date_str or not max_date_str:
            raise Exception("Could not determine date range from dataset.")
            
        print(f"🗓️ Full data range found: {min_date_str} to {max_date_str}")
        return datetime.strptime(min_date_str, "%Y-%m-%d"), datetime.strptime(max_date_str, "%Y-%m-%d")

    # --- Main loop for batch processing ---
    overall_start_dt, overall_end_dt = get_full_date_range()
    
    current_date = overall_start_dt
    while current_date <= overall_end_dt:
        if report_mode == "monthly":
            period_start = current_date.replace(day=1)
            # Find the first day of the next month, then subtract one day to get end of current month
            next_month_start = (period_start + timedelta(days=32)).replace(day=1)
            period_end = min(next_month_start - timedelta(days=1), overall_end_dt)
            report_date = period_start.strftime("%Y-%m")
            # Set the next loop to start at the beginning of the next month
            current_date = next_month_start
        else:  # 'weekly' mode
            period_start = current_date
            period_end = min(current_date + timedelta(days=6), overall_end_dt)
            report_date = period_end.strftime("%Y-%m-%d")
            # Set the next loop to start the day after the current period ends
            current_date = period_end + timedelta(days=1)

        # Call the refactored function to do the work for this specific period
        generate_and_save_report(
            database, table, output_location,
            period_start.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d"),
            report_date,
            report_mode
        )
    
    print("Batch processing complete.")
    return {"statusCode": 200, "body": "Batch report generation completed successfully."}