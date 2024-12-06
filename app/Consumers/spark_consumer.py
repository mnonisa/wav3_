import pyspark
# from app.utils import load_config
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def main():
    # config_ = load_config(file_name='consumer_config.toml')
    spark = SparkSession \
        .builder \
        .appName("spark_consumer") \
        .master("local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", '192.168.50.213:29093') \
        .option("subscribe", 'test-topic') \
        .option('group_id', 'spark_group') \
        .option('enable_auto_commit', False) \
        .option('auto_offset_reset', 'earliest') \
        .load()

    kafka_stream_df = df.selectExpr("CAST(value AS STRING) as message")

    query = kafka_stream_df.writeStream \
        .outputMode("append") \
        .format('console') \
        .start()
    query.awaitTermination()


if __name__ == '__main__':
    main()
