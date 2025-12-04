import pandas as pd
from kafka import KafkaProducer
import json
import time
import sqlite3


db_path = "database/database.sqlite"
conn = sqlite3.connect(db_path)
query = "SELECT * FROM Player_Attributes"
df = pd.read_sql(query, conn)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "rawdata"

for index, row in df.iterrows():
    data = row.to_dict()
    producer.send(topic, data)
    print("\nPlayer ID:", data["player_api_id"])
    print("Data:", data)
    time.sleep(0.5)

conn.close()
print("\n\n--------------------- Finished sending SQL data to Kafka. ------------------------\n\n")