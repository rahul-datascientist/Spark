from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":
    spark = SparkSession \
        .builder \
        .appName("Reading from Kafka") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    # Subscribe to a pattern, at the earliest and latest offsets
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.0.7:9092") \
        .option("subscribePattern", "connect-test") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df.show(500)
    spark.stop()
