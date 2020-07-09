'''
spark/bin/spark-submit \
    --master local --driver-memory 4g \
    --num-executors 2 --executor-memory 4g \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
    sstreaming-spark-out.py
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("Spark Structured Streaming from Kafka") \
    .getOrCreate()
    
    
spark.sparkContext.setLogLevel("OFF")

sdfRides = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.0.7:9092") \
    .option("subscribe", "taxirides") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

sdfFares = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.0.7:9092") \
    .option("subscribe", "taxifares") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

taxiFaresSchema = StructType([ \
    StructField("rideId", LongType()), StructField("taxiId", LongType()), \
    StructField("driverId", LongType()), StructField("startTime", TimestampType()), \
    StructField("paymentType", StringType()), StructField("tip", FloatType()), \
    StructField("tolls", FloatType()), StructField("totalFare", FloatType())])

taxiRidesSchema = StructType([ \
    StructField("rideId", LongType()), StructField("isStart", StringType()), \
    StructField("endTime", TimestampType()), StructField("startTime", TimestampType()), \
    StructField("startLon", FloatType()), StructField("startLat", FloatType()), \
    StructField("endLon", FloatType()), StructField("endLat", FloatType()), \
    StructField("passengerCnt", ShortType()), StructField("taxiId", LongType()), \
    StructField("driverId", LongType())])


def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    col = split(sdf['value'], ',')  # split attributes to nested array in one Column
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])


sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)
sdfFares = parse_data_from_kafka_message(sdfFares, taxiFaresSchema)

query = sdfRides.groupBy("driverId").count()

query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
