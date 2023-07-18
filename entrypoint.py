from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Subscribe to 1 topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
        .option("subscribe", "topic1") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")