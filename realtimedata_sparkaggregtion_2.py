import warnings
warnings.filterwarnings("ignore")
import warnings
warnings.filterwarnings("ignore")
import findspark
findspark.init("/opt/spark")
from statistics import mean

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.5.0 --conf spark.ui.reverseProxy=true --conf spark.ui.reverseProxyUrl=http://192.168.1.104:4040 pyspark-shell'

import sys
import json
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from collections import deque

appName = 'myKinesisApp'
sc = SparkContext(appName=appName)
sc.setLogLevel("WARN")
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

def get_moving_averages(recent_records_queue):
	opens = []
	highs = []
	lows = []
	closes = []
	volumes = []
	for i in recent_records_queue:
		#print(i)
		#print(i.items())
		#print(float(p['4. close'])	 for p in i.values())
		#closes = [float(p['4. close']) for p in i.values()] 
		for key,value in i.items():
			opens.append(float(value['1. open']))
			highs.append(float(value['2. high']))
			lows.append(float(value['3. low']))
			closes.append(float(value['4. close']))
			volumes.append(int(value['5. volume']))
	#print(closes)
	#print(mean(closes))
	m_o = str(mean(opens))
	m_h = str(mean(highs))
	m_l = str(mean(lows))
	m_c = str(mean(closes))
	m_v = str(mean(volumes))
	for key,value in recent_records_queue[-1].items():
		change_percent = ((float(value['4. close']) - float(value['1. open']))/float(value['1. open']))*100
		change_hl_percent = ((float(value['2. high']) - float(value['3. low']))/float(value['3. low']))*100
		print(key+' - Moving average(5 recent records) - Stock price at opening time: '+ m_o)
		print(key+' - Moving average(5 recent records) - Highest Stock price : '+ m_h)
		print(key+' - Moving average(5 recent records) - Lowest Stock price : '+m_l)
		print(key+' - Moving average(5 recent records) - Stock price at closing time: '+m_c)
		print(key+' - Moving average(5 recent records) - Stock volume: '+m_v)
		print(key+' Percent change in stock price from open to close: '+ str(change_percent)+'%')
		print(key+' Percent change in stock price from high to low: '+ str(change_hl_percent)+'%')
	
	#closes = [float(p['4. close']) for p in data.values()]  
	#sma_5 = mean(closes[-5:])
	#sma_20 = mean(closes[-20:])
	#return {'sma_5': sma_5,'sma_20': sma_20}
  
 

# Define your processing logic here
recent_records_queue = deque(maxlen=5)

def process_kinesis_record(record):
	recent_records_queue.append(json.loads(record))
    # Process each Kinesis record
    # Example: Print the record to the console
	print("Received Kinesis Record:", record)
	#print("Aggregation")
	#print(recent_records_queue)
	get_moving_averages(recent_records_queue)
	#print("End of Aggregation")
    


# Apply the processing logic to the Kinesis DStream
kinesisstream.foreachRDD(lambda rdd: rdd.foreach(process_kinesis_record))

# Start the streaming context
ssc.start()

# Wait for the termination of the streaming context (or stop it manually)
ssc.awaitTermination()

