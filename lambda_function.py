import json
import logging
import base64
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3 import session

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS credentials for OpenSearch
aws_session = session.Session()
credentials = aws_session.get_credentials()
region = aws_session.region_name
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)

# OpenSearch client setup
host = 'search-stockdata-egl4wuze7m7vjowogsvudbzx5q.us-east-1.es.amazonaws.com'  # Your OpenSearch domain endpoint, e.g., search-mydomain.us-west-1.es.amazonaws.com
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    # Process each record from the Kinesis stream
    for record in event['Records']:
        try:
            # Kinesis data is base64 encoded so decode here
            payload = base64.b64decode(record["kinesis"]["data"])
            logger.info("Decoded payload: %s", payload)

            # Convert JSON data to a Python dictionary
            data = json.loads(payload)
            logger.info("JSON data: %s", data)

            # Send data to OpenSearch
            # Use the Kinesis sequence number as the document ID to avoid duplicates
            sequence_number = record["kinesis"]["sequenceNumber"]
            index_response = opensearch_client.index(
                index='stock_data',  # The name of the index in OpenSearch, e.g., 'stock-data'
                id=sequence_number,  # Unique ID for the document
                body=json.dumps(data),
                refresh=True  # Optional: set to 'wait_for' if you need it to be searchable immediately
            )
            logger.info("Successfully sent data to OpenSearch. Response: %s", index_response)

        except Exception as e:
            error_message = f"Error processing record: {str(e)}"
            logger.error(error_message)

    return {'statusCode': 200, 'body': json.dumps("Processing complete.")}
