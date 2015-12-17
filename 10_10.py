from __future__ import division
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys
import random
from apache_log_parser import ApacheAccessLog

random.seed(15)


if len(sys.argv) != 2:
    print('Please provide the path to Apache log file')
    print('10_10.py <path_to_log_directory>')
    sys.exit(2)



conf = (SparkConf()
        .setMaster("local[4]")
        .setAppName("log processor")
        .set("spark.executor.memory", "2g"))

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 2)   # Create a streaming context with batch interval of 1 sec
ssc.checkpoint("checkpoint")

####### Example 10-10. #######
# map() and reduceByKey() on DStream 
# Run this example, and then copy the file to 
directory = sys.argv[1]
print(directory)

# create DStream from text file
# Note: the spark streaming checks for any updates to this directory.
# So first, start this program, and then copy the log file logs/access_log.log to 'directory' location
log_data = ssc.textFileStream(directory)

# Parse each line using a utility class
access_log_dstream = log_data.map(ApacheAccessLog.parse_from_log_line).filter(lambda parsed_line: parsed_line is not None)

# map each ip with value 1. So the stream becomes (ip, 1)
ip_dstream = access_log_dstream.map(lambda parsed_line: (parsed_line.ip, 1)) 

ip_count = ip_dstream.reduceByKey(lambda x,y: x+y)
ip_count.pprint(num = 30)



####### Example 10-12 #######
# Join two Dstreams

ip_bytes_dstream = access_log_dstream.map(lambda parsed_line: (parsed_line.ip, parsed_line.content_size))
ip_bytes_sum_dstream = ip_bytes_dstream.reduceByKey(lambda x,y: x+y)
ip_bytes_request_count_dstream = ip_count.join(ip_bytes_sum_dstream)
ip_bytes_request_count_dstream.pprint(num = 30)



####### Example 10-14 #######
def extractOutliers(rdd):
    """ Currently, no logic implemented. But you can specify any rdd logic here"""
    return rdd

transformed_access_log_dstream = access_log_dstream.transform(extractOutliers)
transformed_access_log_dstream.pprint()



######## Example 10-17 #######
# How to use window()to count data over a window
access_logs_window = access_log_dstream.window(windowDuration = 6, slideDuration=4) 
window_counts = access_logs_window.count()
print( " Window count: ")
window_counts.pprint()



######## Example 10-19 #######
# Ip counts per window
ip_count_dstream = ip_dstream.reduceByKeyAndWindow(func = lambda x,y: x+y, invFunc = lambda x,y: x-y, windowDuration = 6, slideDuration=4)
ip_count_dstream.pprint(num=30)



######## Example 10-21 #######
# Windowed count operation
ip_dstream = access_log_dstream.map(lambda entry: entry.ip)
ip_address_request_count = ip_dstream.countByValueAndWindow(windowDuration = 6, slideDuration=4)
ip_address_request_count.pprint()
request_count = access_log_dstream.countByWindow(windowDuration = 6, slideDuration=4)
request_count.pprint()



######## Example 10-23. ########
# Running count of response codes using updateStateByKey() in Scala
# This basically maintains a running sum , rather than sum in windows

def state_full_sum(new_values, global_sum):
    return sum(new_values) + (global_sum or 0)

response_code_dstream = access_log_dstream.map(lambda entry: (entry.response_code, 1))
response_code_count_dstream = response_code_dstream.updateStateByKey(state_full_sum)
response_code_count_dstream.pprint()


###### Example 10-26 #####
ip_address_request_count.saveAsTextFiles(prefix = "outputDir", suffix = "txt")

# Start our streaming context and wait for it to "finish"
ssc.start() 
# Wait for the job to finish
ssc.awaitTermination()