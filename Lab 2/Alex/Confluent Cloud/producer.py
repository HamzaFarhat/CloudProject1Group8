from kafka import KafkaProducer
import time
from datetime import datetime

TOPIC="lab2"
USERNAME="FIAY6ZF6OFAZJHI6"
PASSWORD='1TymL9BFJyTuUAmBF8jZsjL4KYTlFezMfUu4OBcypeDZE1q+lxQ7Cc9mwFZyO78Y'

producer = KafkaProducer(bootstrap_servers="pkc-ld537.ca-central-1.aws.confluent.cloud:9092", sasl_plain_username=USERNAME, sasl_plain_password=PASSWORD, sasl_mechanism="PLAIN", security_protocol="SASL_SSL")

for _ in range(10):
    currentTime=datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    producer.send(topic=TOPIC, value=currentTime.encode())
    print("Sent \"", currentTime, "\" on topic " + TOPIC)
    time.sleep(1)

producer.close()