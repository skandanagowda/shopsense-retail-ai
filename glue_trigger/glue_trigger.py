import boto3

def lambda_handler(event, context):
    print("Lambda triggered by new CSV upload!")

    glue = boto3.client('glue')
    crawler_name = 'retail_cleaned_crawler'

    #Only trigger for cleaned data uploads
    key = event['Records'][0]['s3']['object']['key']
    if not key.startswith("retail-cleaned-data/"):
        print(f"Ignoring file not in cleaned data folder: {key}")
        return {
            'statusCode': 200,
            'body': 'Ignored non-cleaned upload.'
        }

    try:
        # Check crawler status
        response = glue.get_crawler(Name=crawler_name)
        status = response['Crawler']['State']

        if status == 'READY':
            glue.start_crawler(Name=crawler_name)
            print(f"Crawler '{crawler_name}' started.")
            return {
                'statusCode': 200,
                'body': f"Crawler '{crawler_name}' started successfully."
            }
        else:
            print(f"⚠️ Crawler is already running or not ready. Status: {status}")
            return {
                'statusCode': 200,
                'body': f"Crawler is currently {status}. Skipping start."
            }

    except Exception as e:
        print(f"Failed to start crawler: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }