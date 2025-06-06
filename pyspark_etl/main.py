from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import os

incident_schema = StructType([
    StructField("incident_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("crash_date", StringType()),
    StructField("crash_time", StringType()),
    StructField("borough", StringType()),
    StructField("zip_code", StringType()),
    StructField("on_street_name", StringType()),
    StructField("off_street_name", StringType()),
    StructField("cross_street_name", StringType()),
    StructField("vehicle_types", ArrayType(StringType())),
    StructField("contributing_factors", ArrayType(StringType())),
    StructField("number_of_persons_injured", IntegerType()),
    StructField("number_of_persons_killed", IntegerType()),
    StructField("number_of_motorist_injured", IntegerType()),
    StructField("number_of_motorist_killed", IntegerType()),
    StructField("number_of_cyclist_injured", IntegerType()),
    StructField("number_of_cyclist_killed", IntegerType()),
    StructField("number_of_pedestrians_injured", IntegerType()),
    StructField("number_of_pedestrians_killed", IntegerType())
])

spark = SparkSession.builder \
    .appName("KafkaToPostgresETL") \
    .getOrCreate()


df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_incidents") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), incident_schema).alias("data")) \
    .select("data.*")

df_json.printSchema()


# normalizing dim_vehicle

from pyspark.sql.functions import explode, sha2, concat_ws

df_vehicle = df_json.select(explode("vehicle_types").alias("vehicle_type"))

df_vehicle = df_vehicle.dropna().dropDuplicates()

df_vehicle = df_vehicle.withColumn(
    "vehicle_type_id",
    sha2(col("vehicle_type"), 256)
)

def write_dim_vehicle(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", os.getenv("PG_URL")) \
        .option("dbtable", "dim_vehicle") \
        .option("user", os.getenv("PG_USER")) \
        .option("password", os.getenv("PG_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 5. Stream to PostgreSQL
df_vehicle.writeStream \
    .foreachBatch(write_dim_vehicle) \
    .outputMode("update") \
    .start()

