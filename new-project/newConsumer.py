# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, current_timestamp, avg
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
# from time import sleep

# # ---------------------------
# # 1. Spark session
# # ---------------------------
# spark = SparkSession.builder \
#     .appName("PlayerTry") \
#     .config("spark.sql.execution.arrow.enabled", "true") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # ---------------------------
# # 2. JSON Schema for Player_Attributes
# # ---------------------------
# json_schema = StructType([
#     StructField("player_api_id", IntegerType(), True),
#     StructField("overall_rating", FloatType(), True),
#     StructField("potential", FloatType(), True),
#     StructField("finishing", FloatType(), True),
#     StructField("short_passing", FloatType(), True),
#     StructField("dribbling", FloatType(), True),
#     StructField("vision", FloatType(), True),
#     StructField("long_shots", FloatType(), True),
#     StructField("shot_power", FloatType(), True),
#     StructField("acceleration", FloatType(), True),
#     StructField("sprint_speed", FloatType(), True),
#     StructField("strength", FloatType(), True),
#     StructField("stamina", FloatType(), True),
# ])

# # ---------------------------
# # 3. Read stream from Kafka
# # ---------------------------
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "cleandata") \
#     .option("startingOffsets", "latest") \
#     .load()

# # try with casting and without casting
# query = df_raw.selectExpr("CAST(value AS STRING)") \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .option("checkpointLocation", "D:/Projects/big-data/checkpoints/try") \
#     .start()
    
# print("______ WE ARE WORKING ________")
# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round as spark_round, avg, count, max, min, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# Create Spark session with PostgreSQL driver
spark = SparkSession.builder \
    .appName("PlayerAnalytics") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://localhost:5432/playerdb"
postgres_properties = {
    "user": "postgres",
    "password": "ayman@123",
    "driver": "org.postgresql.Driver"
}

# JSON Schema
json_schema = StructType([
    StructField("player_api_id", IntegerType(), True),
    StructField("overall_rating", FloatType(), True),
    StructField("potential", FloatType(), True),
    StructField("finishing", FloatType(), True),
    StructField("short_passing", FloatType(), True),
    StructField("dribbling", FloatType(), True),
    StructField("vision", FloatType(), True),
    StructField("long_shots", FloatType(), True),
    StructField("shot_power", FloatType(), True),
    StructField("acceleration", FloatType(), True),
    StructField("sprint_speed", FloatType(), True),
    StructField("strength", FloatType(), True),
    StructField("stamina", FloatType(), True),
])

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cleandata") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), json_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# Calculate derived fields for raw data
df_enriched = df_parsed \
    .withColumn("passing_skill", spark_round((col("short_passing") + col("vision")) / 2, 2)) \
    .withColumn("physicality", spark_round((col("strength") + col("stamina")) / 2, 2)) \
    .withColumn("shooting_skill", spark_round((col("finishing") + col("shot_power") + col("long_shots")) / 3, 2)) \
    .withColumn("speed_score", spark_round((col("acceleration") + col("sprint_speed")) / 2, 2)) \
    .withColumn("processing_time", current_timestamp())


# ============================================
# TABLE 1: Raw Data (No Aggregation)
# ============================================
def write_raw_to_postgres(batch_df, batch_id):
    """Write raw player data to PostgreSQL"""
    if batch_df.count() > 0:
        print(f"Writing batch {batch_id} - {batch_df.count()} records to raw_player_data table")
        batch_df.write \
            .jdbc(url=postgres_url,
                  table="raw_player_data",
                  mode="overwrite",
                  properties=postgres_properties)
        print(f"Batch {batch_id} written successfully!")

query_raw = df_enriched.writeStream \
    .foreachBatch(write_raw_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/raw_data") \
    .start()


# ============================================
# TABLE 2: Aggregated Data
# ============================================
def write_aggregated_to_postgres(batch_df, batch_id):
    """Write aggregated player statistics to PostgreSQL"""
    if batch_df.count() > 0:
        print(f"Aggregating batch {batch_id}...")
        
        # Aggregate by player_api_id
        aggregated_df = batch_df.groupBy("player_api_id").agg(
            count("*").alias("num_records"),
            avg("overall_rating").alias("avg_overall_rating"),
            max("overall_rating").alias("max_overall_rating"),
            min("overall_rating").alias("min_overall_rating"),
            avg("potential").alias("avg_potential"),
            avg("passing_skill").alias("avg_passing_skill"),
            avg("physicality").alias("avg_physicality"),
            avg("shooting_skill").alias("avg_shooting_skill"),
            avg("speed_score").alias("avg_speed_score"),
            max("processing_time").alias("last_updated")
        ).withColumn("avg_overall_rating", spark_round(col("avg_overall_rating"), 2)) \
         .withColumn("avg_potential", spark_round(col("avg_potential"), 2)) \
         .withColumn("avg_passing_skill", spark_round(col("avg_passing_skill"), 2)) \
         .withColumn("avg_physicality", spark_round(col("avg_physicality"), 2)) \
         .withColumn("avg_shooting_skill", spark_round(col("avg_shooting_skill"), 2)) \
         .withColumn("avg_speed_score", spark_round(col("avg_speed_score"), 2))
        
        print(f"Writing aggregated data for batch {batch_id} to aggregated_player_stats table")
        aggregated_df.write \
            .jdbc(url=postgres_url,
                  table="aggregated_player_stats",
                  mode="overwrite",
                  properties=postgres_properties)
        print(f"Aggregated batch {batch_id} written successfully!")

query_agg = df_enriched.writeStream \
    .foreachBatch(write_aggregated_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/aggregated_data") \
    .start()


# Wait for both queries
print("\nStreaming started!")
print("____________ Raw data raw_player_data table _______________")
print("____________ Aggregated data aggregated_player_stats table _______________")
print("\nPress Ctrl+C to stop...")

spark.streams.awaitAnyTermination()