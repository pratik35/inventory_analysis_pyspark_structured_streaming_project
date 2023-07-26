from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import os
import sys

KAFKA_INPUT_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def foreach_batch_function(df,batchId):
    #df.show()
    print("Count:"+str(df.count())+" for BatchId:"+str(batchId))
    df = df.groupBy('VendorName').agg(
        sum('PurchasePrice').alias('Total_Cost'), \
        sum('Volume').alias('Total_Volume') \
    )
    df.show(truncate=False)
    #df.coalesce(1).write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/input')

def read_schema():
    f = open('schema.json')
    json_load = json.load(f)
    schema = StructType()
    for i in json_load['fields']:
        field=i['name']
        nullable=i['nullable']
        if i['type'] == "string":
            dataType=StringType()
        elif i['type'] == "integer":
            dataType=IntegerType()
        else:
            dataType=DecimalType()
        schema.add(i['name'],dataType,nullable)
    return schema

if __name__ == "__main__":
    print("Reading Messages from Kafka topic about to start.....")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").config("spark.ui.port","4050").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    
    
    
    schema = read_schema()
    read=spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "true") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    
    print("Printing schema of stream:")
    read.printSchema()
    read = read.selectExpr("CAST(value as STRING)","timestamp") \
                                        .select(from_json(col("value"),schema).alias("detail"),"timestamp","value") \
                                        .select("detail.*","timestamp","value")
    
    write_stream= read \
                  .writeStream \
                  .outputMode("append") \
                  .foreachBatch(foreach_batch_function) \
                  .trigger(processingTime="1 minutes") \
                  .start() \
                  .awaitTermination()
    

    #read.writeStream.format("console") won't print the dataset when we we are using foreachBatch
    #.trigger(processingTime="1 minutes") will check for new messages on the Kafka stream every 1 minutes 
    #.option("maxOffsetsPerTrigger", 1000) \ reads 1000 messages per batch
    #.option("startingOffsets", "earliest") \ will read messages present in the kafka queue and 'latest' will read only after consumer is started and if we send messages after consumer has started