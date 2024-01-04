import findspark
findspark.init("/opt/spark")

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.5.0 pyspark-shell'

import sys
import json
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

appName = 'myKinesisApp'
sc = SparkContext(appName=appName)
ssc = StreamingContext(sc, 1)

streamName = 'Kinesis_stock'
endpointURL = 'https://kinesis.us-east-2.amazonaws.com'  # Replace with your Kinesis endpoint URL
regionName = 'us-east-2'
AWS_ACCESS_KEY = 'AKIAWLCZSFZI4NTZYTUO'
SECRET_ACCESS_KEY= '8tSe2sYgkSsetx3dDPOjxf0/1Frf//BqI6IqWqyN'
checkpointInterval = 5
initialPosition = InitialPositionInStream.LATEST  # Change this to INITIAL_POSITION_IN_STREAM to specify the initial position


kinesisstream = KinesisUtils.createStream(
    ssc,
    appName,
    streamName,
    endpointURL,
    regionName,
    initialPosition, 
    awsAccessKeyId=AWS_ACCESS_KEY,
    awsSecretKey=SECRET_ACCESS_KEY,
    checkpointInterval=checkpointInterval)

# Define your processing logic here
def process_kinesis_record(record):
    # Process each Kinesis record
    # Example: Print the record to the console
    print("Received Kinesis Record:", record)

# Apply the processing logic to the Kinesis DStream
kinesisstream.foreachRDD(lambda rdd: rdd.foreach(process_kinesis_record))

# Start the streaming context
ssc.start()

# Wait for the termination of the streaming context (or stop it manually)
ssc.awaitTermination()

