from pyspark.sql import SparkSession
import yaml
from yaml.loader import SafeLoader
from src.transform.transform import Transfom
from src.utils import config_logger
import logging

if __name__ == '__main__':

    logger = config_logger()
    logger.setLevel(logging.INFO)

    with open('config.yml') as f:
        data = yaml.load(f, Loader=SafeLoader)

    logger.debug("Starting %s process", data['pipeline_id'])
    spark = SparkSession \
        .builder \
        .appName(data["pipeline_id"]) \
        .getOrCreate()
    # Subscribe to 1 topic

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", data["source"]["hosts"]) \
        .option("subscribe", data["source"]["topic"]) \
        .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df_transformed = Transfom.run(logger=logger, df=df)
    df_transformed.writeStream \
        .format("kafka") \
        .option("checkpointLocation", data["destination"]["checkpoint"]) \
        .option("kafka.bootstrap.servers", data["destination"]["hosts"]) \
        .option("topic", data["destination"]["topic"]) \
        .trigger(continuous="1 second").start().awaitTermination()
