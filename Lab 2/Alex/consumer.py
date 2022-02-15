from kafka import KafkaConsumer

TOPIC="lab2"

consumer = KafkaConsumer(bootstrap_servers="0.0.0.0:9094")

topics = []
topics.append(TOPIC)
consumer.subscribe(topics=topics)

for message in consumer:
    message = message.value.decode()
    print(message)

consumer.close()