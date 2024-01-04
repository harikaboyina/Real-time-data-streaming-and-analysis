import json
import logging
import requests
import boto3
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    alpha_vantage_api_key = os.environ.get('ALPHA_VANTAGE_API_KEY', 'EO97PFM4WD83PJV8')
    kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME', 'stock_stream_data')
    alpha_vantage_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&apikey={alpha_vantage_api_key}'

    # Fetch data from Alpha Vantage
    try:
        response = requests.get(alpha_vantage_url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error('Error fetching data from Alpha Vantage: %s', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps('Error fetching data from Alpha Vantage.')
        }

    # Send the entire data to Kinesis
    try:
        formatted_data = json.dumps(data).encode('utf-8')
        # Log the data being sent to Kinesis
        logger.info('Sending the following data to Kinesis: %s', formatted_data)

        kinesis_response = kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=formatted_data,
            PartitionKey='partition-key'  # Replace with an appropriate partition key
        )
        logger.info('Successfully sent data to Kinesis. Response: %s', kinesis_response)

    except Exception as e:
        logger.error('Error sending data to Kinesis: %s', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps('Error sending data to Kinesis.')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Data sent to Kinesis successfully!')
    }

# Be sure to replace 'YOUR_DEFAULT_API_KEY' with your actual Alpha Vantage API key.
