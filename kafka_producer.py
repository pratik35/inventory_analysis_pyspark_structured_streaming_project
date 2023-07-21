from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from kafka import KafkaProducer
import time
import os
import sys
from json import dumps


KAFKA_INPUT_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    df = spark.read.csv("Dataset/2017PurchasePricesDec.csv",header=True,inferSchema=True) 
    print(type(df.schema.names))
    dataCollect = df.collect()
    message = None
    for row in dataCollect:
      message = {}
      for i in df.schema.names:
        message[str(i)] = row[i]
      print("Message to be sent: ", message)
      kafka_producer_obj.send(KAFKA_INPUT_TOPIC_NAME_CONS, message)
      #time.sleep(10)
    