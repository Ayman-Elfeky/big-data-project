from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, from_csv, avg, current_timestamp
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
# 2. CSV Schema
# ---------------------------------
schema = """
Rk INT,
Player STRING,
Nation STRING,
Pos STRING,
Squad STRING,
Age INT,
Born INT,
MP INT,
Starts INT,
Min INT,
n90s FLOAT,
Gls INT,
Ast INT,
G_A INT,
G_PK INT,
PK INT,
PKatt INT,
CrdY INT,
CrdR INT,
xG FLOAT,
npxG FLOAT,
xAG FLOAT,
npxG_xAG FLOAT,
PrgC INT,
PrgP INT,
PrgR INT,
Gls_1 FLOAT,
Ast_1 FLOAT,
G_A_1 FLOAT,
G_PK_1 FLOAT,
G_A_PK FLOAT,
xG_1 FLOAT,
xAG_1 FLOAT,
xG_xAG FLOAT,
npxG_1 FLOAT,
npxG_xAG_1 FLOAT
"""
print("----- Schema defined -----")

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

query = df_string.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()
    
print("----- Kafka value converted to string -----")

# ---------------------------------
# 4. Parse CSV
# ---------------------------------
df = df_string.select(from_csv(col("value"), schema).alias("data")).select("data.*")

print("----- CSV parsed -----")

# ---------------------------------
# 5. Normalize field names
# ---------------------------------
df = df.withColumnRenamed("n90s", "90s") \
       .withColumnRenamed("G_A", "G_plus_A") \
       .withColumnRenamed("G_PK", "G_minus_PK") \
       .withColumnRenamed("npxG_xAG", "npxG_plus_xAG") \
       .withColumnRenamed("G_A_1", "G_plus_A_1") \
       .withColumnRenamed("G_PK_1", "G_minus_PK_1") \
       .withColumnRenamed("G_A_PK", "G_plus_A_minus_PK") \
       .withColumnRenamed("xG_xAG", "xG_plus_xAG") \
       .withColumnRenamed("npxG_xAG_1", "npxG_plus_xAG_1")

print("----- Field names normalized -----")

# # ---------------------------------
# # 6. Cast numeric fields
# # ---------------------------------
print(F"\n\n\n\n----------------- DF.dtypes: {df.dtypes} ------------------------\n\n\n\n")
numeric_cols = [c for c, t in df.dtypes if t == "string" and c not in ["Player", "Nation", "Pos", "Squad"]]
print(f"\n\n\n\n------------------ Numeric columns: {numeric_cols} ----------------------------\n\n\n\n")
for col_name in numeric_cols:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# DF.dtypes: [('Rk', 'int'), ('Player', 'string'), ('Nation', 'string'), ('Pos', 'string'), ('Squad', 'string'), ('Age', 'int'), ('Born', 'int'), ('MP', 'int'), ('Starts', 'int'), ('Min', 'int'), ('90s', 'float'), ('Gls', 'int'), ('Ast', 'int'), ('G_plus_A', 'int'), ('G_minus_PK', 'int'), ('PK', 'int'), ('PKatt', 'int'), ('CrdY', 'int'), ('CrdR', 'int'), ('xG', 'float'), ('npxG', 'float'), ('xAG', 'float'), ('npxG_plus_xAG', 'float'), ('PrgC', 'int'), ('PrgP', 'int'), ('PrgR', 'int'), ('Gls_1', 'float'), ('Ast_1', 'float'), ('G_plus_A_1', 'float'), ('G_minus_PK_1', 'float'), ('G_plus_A_minus_PK', 'float'), ('xG_1', 'float'), ('xAG_1', 'float'), ('xG_plus_xAG', 'float'), ('npxG_1', 'float'), ('npxG_plus_xAG_1', 'float')] --

print("----- Numeric fields casted -----")

# ---------------------------------
# 7. Extract Nation code
# ---------------------------------
df = df.withColumn("Nation", f"check {split(col("Nation"), " ").getItem(1)}")
print("----- Nation codes extracted -----")

# ---------------------------------
# 8. Player analytics
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
# 9. Team analytics
# ---------------------------------
team_summary = df_analytics.groupBy("Squad").agg(
    avg("shooting_accuracy").alias("avg_shooting_accuracy"),
    avg("passing_efficiency").alias("avg_passing_efficiency"),
    avg("defensive_score").alias("avg_defensive_score"),
    avg("overall_contribution").alias("avg_overall_contribution")
).withColumn("event_time", current_timestamp())

print("----- Team analytics aggregated -----")

# ---------------------------------
# 10. PostgreSQL connection
# ---------------------------------
postgres_url = "jdbc:postgresql://localhost:5432/playerdb"
postgres_properties = {
    "user": "postgres",
    "password": "ayman@123",
    "driver": "org.postgresql.Driver"
}

print("----- PostgreSQL connection configured -----")

# ---------------------------------
# 11. Safe write function
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
# 12. Streaming writes
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
# 13. Await termination
# ---------------------------------
player_query.awaitTermination()
team_query.awaitTermination()
