from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import avro.schema
from avro.datafile import DataFileReader
from kafka import KafkaConsumer
from json import loads
import time
import os
import sys

KAFKA_INPUT_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    print("Reading Messages from Kafka topic about to start.....")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    schema = StructType([
        StructField(name='Brand',dataType=StringType()),
        StructField(name='Description',dataType=StringType()),
        StructField(name='Price',dataType=StringType()),
        StructField(name='Size',dataType=StringType()),
        StructField(name='Volume',dataType=StringType()),
        StructField(name='Classification',dataType=StringType()),
        StructField(name='PurchasePrice',dataType=StringType()),
        StructField(name='VendorNumber',dataType=StringType()),
        StructField(name='VendorName',dataType=StringType())
    ])


    read=spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()
    
    print("Printing schema of stream:")
    read.printSchema()
    read = read.selectExpr("CAST(value as STRING)","timestamp") \
                                        .select(from_json(col("value"),schema).alias("detail"),"timestamp","value") \
                                        .select("detail.*","timestamp","value")
    
    write_stream= read \
                  .writeStream \
                  .outputMode("append") \
                  .format("console") \
                  .start() \
                  .awaitTermination()