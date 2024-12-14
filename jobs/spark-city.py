# Used for listening events from Kafka
# 9090:8080 is where all the UI is for spark master
# 7077:7077 is where we are submitting all our spark jobs

# from narwhals import col
# from pydantic_core import from_json
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.jars.packages", "com.amazonaws:aws-java-sdk:1.11.469") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()


    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])
    
    # gps schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    # traffic schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    # weather schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    # emergency schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                    .format('kafka')
                    .option('kafka.bootstrap.servers', 'broker:29092')
                    .option('subscribe', topic)
                    .option('startingOffsets', 'earliest')
                    .load()
                    .selectExpr('CAST(value AS STRING)')
                    .select(from_json(col('value'), schema).alias('data'))
                    .select('data.*')
                    .withWatermark('timestamp', '2 minutes')
                    )
    
    def streamWriter(input: DataFrame, checkpointFolder, output): 
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gps_schema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    query1 = streamWriter(vehicleDF, 's3a://awsbucketsmartcityproject/checkpoints/vehicle_data', 's3a://awsbucketsmartcityproject/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://awsbucketsmartcityproject/checkpoints/gps_data', 's3a://awsbucketsmartcityproject/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://awsbucketsmartcityproject/checkpoints/traffic_data', 's3a://awsbucketsmartcityproject/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://awsbucketsmartcityproject/checkpoints/weather_data', 's3a://awsbucketsmartcityproject/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://awsbucketsmartcityproject/checkpoints/emergency_data', 's3a://awsbucketsmartcityproject/data/emergency_data')

    query5.awaitTermination()
if __name__ == "__main__":
    main()