from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
CHECKPOINT_DIR  = '/opt/spark_stream/checkpoints/run_v1' #'/tmp/checkpoints/created_users'

CASSANDRA_USER = os.getenv('CASSANDRA_USER', 'cassandra')
CASSANDRA_PASS = os.getenv('CASSANDRA_PASSWORD', 'cassandra')

def create_spark_connection():
    return SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config(
            'spark.jars.packages',
            'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,'
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .config('spark.jars.ivy', '/tmp/.ivy2') \
        .config('spark.cassandra.connection.host', 'cassandra') \
        .config('spark.cassandra.auth.username', CASSANDRA_USER) \
        .config('spark.cassandra.auth.password', CASSANDRA_PASS) \
        .config('spark.cassandra.output.batch.size.bytes', '1024') \
        .config('spark.cassandra.output.concurrent.writes', '1') \
        .getOrCreate()

def read_kafka_stream(spark):
    return spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'users_created') \
        .option('startingOffsets', 'earliest') \
        .load()

def parse_kafka(df):
    schema = StructType([
        StructField("id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("gender", StringType()),
        StructField("address", StringType()),
        StructField("post_code", StringType()),
        StructField("email", StringType()),
        StructField("username", StringType()),
        StructField("registered_date", StringType()),
        StructField("phone", StringType()),
        StructField("picture", StringType())
    ])

    return df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

def write_to_cassandra(df):
    return df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option('checkpointLocation', CHECKPOINT_DIR) \
        .option('keyspace', 'spark_streams') \
        .option('table', 'created_users') \
        .outputMode("append") \
        .start()

if __name__ == "__main__":
    spark = create_spark_connection()
    spark.sparkContext.setLogLevel("INFO")

    kafka_df  = read_kafka_stream(spark)
    parsed_df = parse_kafka(kafka_df)

    query = write_to_cassandra(parsed_df)
    query.awaitTermination()
