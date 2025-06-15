from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import os

incident_schema = StructType([
    StructField("incident_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("crash_date", StringType()),
    StructField("crash_time", StringType()),
    StructField("location_id", StringType()),
    StructField("borough", StringType()),
    StructField("zip_code", StringType()),
    StructField("on_street_name", StringType()),
    StructField("off_street_name", StringType()),
    StructField("cross_street_name", StringType()),
    StructField("vehicle_type_id", StringType()),
    StructField("vehicle_types", ArrayType(StringType())),
     StructField("cont_factors_id", StringType()),
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
    .config("spark.jars.packages", ",".join([
        "org.postgresql:postgresql:42.7.3", 
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1"  
    ])) \
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



from pyspark.sql.functions import explode, sha2, concat_ws,to_date,date_format, year, month, dayofweek,to_timestamp,posexplode

def write_to_postgres(table_name):
    def _write(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", os.getenv("PG_URL")) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("PG_USER")) \
            .option("password", os.getenv("PG_PASSWORD")) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    return _write


# inserting data into dim_vehicle
df_vehicle = df_json.select(
    posexplode("vehicle_types").alias("pos", "vehicle_type"),
    posexplode("vehicle_type_ids").alias("pos2", "vehicle_id")
).where(col("pos") == col("pos2")).select("vehicle_id", "vehicle_type", (col("pos") + 1).alias("vehicle_position"))





df_vehicle.writeStream \
    .foreachBatch(write_to_postgres("dim_vehicle")) \
    .outputMode("update") \
    .start()

# now let us go to date dimension date

df_date = df_json.select(  to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX").alias("date_id"),
    to_date("crash_date", "yyyy-MM-dd").alias("crash_date"),
                         dayofweek(to_date("crash_date", "yyyy-MM-dd")).alias("day_of_week"),
    month(to_date("crash_date", "yyyy-MM-dd")).alias("month"),
    year(to_date("crash_date", "yyyy-MM-dd")).alias("year")
).dropDuplicates(["date_id"])

df_date.writeStream \
    .foreachBatch(write_to_postgres("dim_date")) \
    .outputMode("update") \
    .start()

# for location dimension
df_location = df_json.select(
    col("borough"),
    col("zip_code"),
    col("on_street_name"),
    col("cross_street_name"),
    col("off_street_name")
).dropna(subset=["borough", "on_street_name"])

df_location = df_location.dropDuplicates()


#for contributing factor dimension

df_factors = df_json.select(
    posexplode("contributing_factors").alias("factor_position_zero_based", "factor_description")
).withColumn(
    "factor_position", col("factor_position_zero_based") + 1
).drop("factor_position_zero_based")

df_factors = df_factors.dropna().dropDuplicates(["factor_description", "factor_position"])

df_factors.writeStream \
    .foreachBatch(write_to_postgres("dim_contributing_factor")) \
    .outputMode("update") \
    .start()

