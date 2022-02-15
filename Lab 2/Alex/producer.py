from kafka import KafkaProducer
import time
from datetime import datetime

TOPIC="lab2"

producer = KafkaProducer(bootstrap_servers="0.0.0.0:9093")

for _ in range(10):
    currentTime=datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    producer.send(topic=TOPIC, value=currentTime.encode())
    print("Sent \"", currentTime, "\" on topic " + TOPIC)
    time.sleep(1)

producer.close()