from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from time import sleep

# ---------------------------------
# 1. Spark Session
# ---------------------------------
spark = SparkSession.builder \
    .appName("PlayerAnalytics") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("----- Spark Session started -----")

# ---------------------------------
# 2. JSON Schema
# ---------------------------------
json_schema = StructType([
    StructField("Rk", IntegerType(), True),
    StructField("Player", StringType(), True),
    StructField("Nation", StringType(), True),
    StructField("Pos", StringType(), True),
    StructField("Squad", StringType(), True),
    StructField("Age", FloatType(), True),
    StructField("Born", FloatType(), True),
    StructField("MP", IntegerType(), True),
    StructField("Starts", IntegerType(), True),
    StructField("Min", IntegerType(), True),
    StructField("90s", FloatType(), True),
    StructField("Gls", IntegerType(), True),
    StructField("Ast", IntegerType(), True),
    StructField("G_plus_A", IntegerType(), True),
    StructField("G_minus_PK", IntegerType(), True),
    StructField("PK", IntegerType(), True),
    StructField("PKatt", IntegerType(), True),
    StructField("CrdY", IntegerType(), True),
    StructField("CrdR", IntegerType(), True),
    StructField("xG", FloatType(), True),
    StructField("npxG", FloatType(), True),
    StructField("xAG", FloatType(), True),
    StructField("npxG_plus_xAG", FloatType(), True),
    StructField("PrgC", IntegerType(), True),
    StructField("PrgP", IntegerType(), True),
    StructField("PrgR", IntegerType(), True),
    StructField("Gls_1", FloatType(), True),
    StructField("Ast_1", FloatType(), True),
    StructField("G_plus_A_1", FloatType(), True),
    StructField("G_minus_PK_1", FloatType(), True),
    StructField("G_plus_A_minus_PK", FloatType(), True),
    StructField("xG_1", FloatType(), True),
    StructField("xAG_1", FloatType(), True),
    StructField("xG_plus_xAG", FloatType(), True),
    StructField("npxG_1", FloatType(), True),
    StructField("npxG_plus_xAG_1", FloatType(), True),
])

print("----- JSON Schema defined -----")

# ---------------------------------
# 3. Read from Kafka
# ---------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cleandata") \
    .option("startingOffsets", "earliest") \
    .load()

print("----- Kafka stream loaded -----")
df_raw.printSchema()

# Convert Kafka value to string
df_string = df_raw.selectExpr("CAST(value AS STRING)")
print("----- Kafka value converted to string -----")

# ---------------------------------
# 4. Parse JSON
# ---------------------------------
df = df_string.select(from_json(col("value"), json_schema).alias("data")).select("data.*")
print("----- JSON parsed -----")

# ---------------------------------
# 5. Normalize field names
# ---------------------------------
df = df.withColumnRenamed("90s", "90s") \
       .withColumnRenamed("G_plus_A", "G_plus_A") \
       .withColumnRenamed("G_minus_PK", "G_minus_PK") \
       .withColumnRenamed("npxG_plus_xAG", "npxG_plus_xAG") \
       .withColumnRenamed("G_plus_A_1", "G_plus_A_1") \
       .withColumnRenamed("G_minus_PK_1", "G_minus_PK_1") \
       .withColumnRenamed("G_plus_A_minus_PK", "G_plus_A_minus_PK") \
       .withColumnRenamed("xG_plus_xAG", "xG_plus_xAG") \
       .withColumnRenamed("npxG_plus_xAG_1", "npxG_plus_xAG_1")

print("----- Field names normalized -----")

# ---------------------------------
# 6. Extract Nation code
# ---------------------------------
df = df.withColumn("Nation", split(col("Nation"), " ").getItem(1))
print("----- Nation codes extracted -----")

# ---------------------------------
# 7. Player analytics
# ---------------------------------
df_analytics = df.withColumn(
    "shooting_accuracy",
    when(col("xG") > 0, col("Gls") / col("xG")).otherwise(0)
).withColumn(
    "passing_efficiency",
    when(col("PrgP") > 0, col("Ast") / col("PrgP")).otherwise(0)
).withColumn(
    "defensive_score",
    (col("MP") - (col("CrdY") + 2 * col("CrdR"))) / col("MP")
).withColumn(
    "overall_contribution",
    col("Gls") + col("Ast") + col("PrgP") + col("defensive_score")
).withColumn(
    "event_time",
    current_timestamp()
)

print("----- Player analytics metrics calculated -----")

# ---------------------------------
# 8. Team analytics
# ---------------------------------
team_summary = df_analytics.groupBy("Squad").agg(
    avg("shooting_accuracy").alias("avg_shooting_accuracy"),
    avg("passing_efficiency").alias("avg_passing_efficiency"),
    avg("defensive_score").alias("avg_defensive_score"),
    avg("overall_contribution").alias("avg_overall_contribution")
).withColumn("event_time", current_timestamp())

print("----- Team analytics aggregated -----")

# ---------------------------------
# 9. PostgreSQL connection
# ---------------------------------
postgres_url = "jdbc:postgresql://localhost:5432/playerdb"
postgres_properties = {
    "user": "postgres",
    "password": "ayman@123",
    "driver": "org.postgresql.Driver"
}

print("----- PostgreSQL connection configured -----")

# ---------------------------------
# 10. Safe write function
# ---------------------------------
def safe_write(df_batch, epoch_id, table_name, mode="append"):
    retries = 3
    while retries > 0:
        try:
            print(f"----- Writing batch {epoch_id} to {table_name} ({df_batch.count()} rows) -----")
            df_batch.write.jdbc(
                url=postgres_url,
                table=table_name,
                mode=mode,
                properties=postgres_properties
            )
            print(f"----- Finished writing batch {epoch_id} -----")
            break
        except Exception as e:
            print(f"----- Error writing batch {epoch_id}: {e} -----")
            retries -= 1
            sleep(5)

# ---------------------------------
# 11. Streaming writes
# ---------------------------------
player_query = df_analytics.writeStream \
    .foreachBatch(lambda df, eid: safe_write(df, eid, "player_analytics", "append")) \
    .outputMode("append") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/player") \
    .start()

team_query = team_summary.writeStream \
    .foreachBatch(lambda df, eid: safe_write(df, eid, "team_summary", "overwrite")) \
    .outputMode("complete") \
    .option("checkpointLocation", "D:/Projects/big-data/checkpoints/team") \
    .start()

print("----- Streaming queries started -----")

# ---------------------------------
# 12. Await termination
# ---------------------------------
player_query.awaitTermination()
team_query.awaitTermination()
