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
postgres_url = "jdbc:postgresql://localhost:5432/bigdatafinal"
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
    batch_count = batch_df.count()
    
    if batch_count > 0:
        print(f"\n{'='*60}")
        print(f"RAW DATA - Batch {batch_id}")
        print(f"Writing {batch_count} records to raw_player_data table")
        
        try:
            batch_df.write \
                .jdbc(url=postgres_url,
                      table="raw_player_data",
                      mode="append",
                      properties=postgres_properties)
            print(f"Batch {batch_id} written successfully!")
        except Exception as e:
            print(f"Error writing batch {batch_id}: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print(f"{'='*60}\n")

query_raw = df_enriched.writeStream \
    .foreachBatch(write_raw_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/raw_data") \
    .start()


# ============================================
# TABLE 2: Global Aggregated Data
# ============================================
def write_aggregated_to_postgres(batch_df, batch_id):
    """Aggregate across ALL data (existing + new batch) using Spark"""
    batch_count = batch_df.count()
    
    if batch_count > 0:
        print(f"\n{'='*60}")
        print(f"AGGREGATED DATA - Batch {batch_id}")
        print(f"New batch contains {batch_count} records")
        
        try:
            # Read ALL raw data from the database
            print("Reading all raw player data from database...")
            all_raw_df = spark.read \
                .jdbc(url=postgres_url, 
                      table="raw_player_data",
                      properties=postgres_properties)
            
            total_raw_records = all_raw_df.count()
            print(f"Total raw records in database: {total_raw_records}")
            
            # Aggregate across ALL raw data
            print("Re-aggregating all data...")
            global_aggregated_df = all_raw_df.groupBy("player_api_id").agg(
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
            
            unique_players = global_aggregated_df.count()
            print(f"Unique players after aggregation: {unique_players}")
            
            # Show sample of aggregated data
            print("\Sample aggregated data (top 5):")
            global_aggregated_df.orderBy(col("num_records").desc()).show(5, truncate=False)
            
            # Overwrite the entire aggregated table
            print(f"Writing {unique_players} player records to aggregated_player_stats...")
            global_aggregated_df.write \
                .jdbc(url=postgres_url, 
                      table="aggregated_player_stats", 
                      mode="overwrite",  # Overwrite entire table with fresh calculations
                      properties=postgres_properties)
            
            print(f"\n_____ Successfully updated aggregated stats for {unique_players} players_______")
            print(f"\n_____ Each player has exactly ONE row with cumulative statistics__________")
            
        except Exception as e:
            print(f"Error aggregating batch {batch_id}: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print(f"{'='*60}\n")

query_agg = df_enriched.writeStream \
    .foreachBatch(write_aggregated_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/aggregated_data") \
    .start()


# Wait for both queries
print("\n" + "="*60)
print("Streaming started!")
print("="*60)
print("Raw data -> raw_player_data table (append mode)")
print("Aggregated data -> aggregated_player_stats table (global aggregation)")
print("Each player_api_id has exactly ONE row with cumulative stats")
print("="*60)
print("\nPress Ctrl+C to stop...\n")

spark.streams.awaitAnyTermination()