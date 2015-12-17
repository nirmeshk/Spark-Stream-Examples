### About
These are the python translations of the examples of *chapter 10 : Spark Streamming* from the book 
**Learning Spark : Lightning-fast Data Analytics** *By Karau, Holden, Zaharia, Matei, Wendell, Patrick, Konwinski, Andy*

### Instructions:

- There are 2 different files for the solutions:
    + 10_4.py : This contains the starting example 10-4 using sockets as input source
    + 10_10.p : This contains all the other examples. The reason for keeping them in same files is that they all work on same initial DStream.

- After these examples, the other examples are for Kafka and flume sources. The Kafka example is directly used in one of the streaming projects. 

- Note: Pyspark api for spark-streamming does not allow defining your own source for stream data.

- Instructions on how to run the Spark Streaming python examples:
    ```bash
    
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 --master local[4] <Python File> [optional command line parameters] 
    
    # Example:
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 --master local[4] 10_4.py

    # Example:
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 --master local[4] 10_10.py logs/
    
    ```
### References:
- Learning Spark : Lightning-fast Data Analytics -By Karau, Holden, Zaharia, Matei, Wendell, Patrick, Konwinski, Andy
- A great reference for Spark Streaming Python API: http://spark.apache.org/docs/latest/api/python/pyspark.streaming.html
- Reference for Pyspark api: http://spark.apache.org/docs/latest/api/python/index.

