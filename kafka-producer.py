# from kafka import KafkaProducer

# # Create a Kafka producer instance
# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# # Produce messages to a Kafka topic
# # Read CSV
# for i in range(1, 11):
#     message = f"Message {i}"
#     producer.send('rawdata', value=message.encode('utf-8'))

# # Close the producer connection
# producer.close()


import pandas as pd
from kafka import KafkaProducer
import json
import time

# 1️⃣ Load the CSV file
df = pd.read_csv("data.csv")   # put your CSV name here

# 2️⃣ Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "rawdata"

# 3️⃣ Send each row of the CSV to Kafka
for index, row in df.iterrows():
    record = row.to_dict()  
    
    producer.send(topic, value=record)
    print(f"Sent record {index+1}: {str(record).encode('utf-8')}")


    time.sleep(0.5)  # Optional: slow down sending (0.5 second)

producer.flush()
print("Finished sending CSV data to Kafka.")
