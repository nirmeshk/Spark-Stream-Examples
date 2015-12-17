from __future__ import division
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
import random

random.seed(15)

conf = (SparkConf()
        .setMaster("local[4]")
        .setAppName("SentimentAnalysis")
        .set("spark.executor.memory", "2g"))

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 2)   # Create a streaming context with batch interval of 1 sec
ssc.checkpoint("checkpoint")

# Example 10-4. Streaming filter for printing lines containing "error" in Python
lines = ssc.socketTextStream("localhost", 7777)
error_lines = lines.filter(lambda x: "error" in x)
error_lines.pprint()

# Start our streaming context and wait for it to "finish"
ssc.start() 
# Wait for the job to finish
ssc.awaitTermination()

# Once you start the spark job, provide its input via a socket using ncat.
# sudo apt-get install nmap
# Start the socket on ubuntu using: $ nc -lk 7777   
# Type in the lines, the lines that contains "error" will be printed on respective interval on the terminal where this spark program is running