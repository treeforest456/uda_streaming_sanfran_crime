import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

"""
Questions:
1. What are the top types of crimes in San Fransisco?
2. What is the crime density by location?
"""



KAFKA_PORT = 9092
TOPIC_NAME = "crime_topic"
# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

"""
{'crime_id': '183653717', 'original_crime_type_name': 'Fight No Weapon', 'report_date': '2018-12-31T00:00:00.000', 'call_date': '2018-12-31T00:00:00.000', 'offense_date': '2018-12-31T00:00:00.000', 'call_time': '23:33', 'call_date_time': '2018-12-31T23:33:00.000', 'disposition': 'GOA', 'address': '5th St/minna St', 'city': 'San Francisco', 'state': 'CA', 'agency_id': '1', 'address_type': 'Intersection', 'common_location': ''}
"""

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"localhost:{KAFKA_PORT}") \
        .option("subscribe", f"{TOPIC_NAME}") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    
    

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select('original_crime_type_name', 'disposition', 'call_date_time').distinct().withWatermark('call_date_time', '1 minute')

    # count the number of original crime type
    agg_df = distinct_table.select('original_crime_type_name').groupby('original_crime_type_name').count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
            .writeStream \
            .trigger(processingTime="10 seconds") \
            .outputMode("Complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()
    
    '''
        query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("Complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    '''


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition, "inner")


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
