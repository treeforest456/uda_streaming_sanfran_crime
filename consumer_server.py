from kafka import KafkaConsumer
import json
import time


consumer = KafkaConsumer("crime_topic", bootstrap_servers=["localhost:9092"])
for message in consumer:
    print(message.value)
        