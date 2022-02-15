from kafka import KafkaConsumer

TOPIC="lab2"
USERNAME="FIAY6ZF6OFAZJHI6"
PASSWORD='1TymL9BFJyTuUAmBF8jZsjL4KYTlFezMfUu4OBcypeDZE1q+lxQ7Cc9mwFZyO78Y'

consumer = KafkaConsumer(bootstrap_servers="pkc-ld537.ca-central-1.aws.confluent.cloud:9092", sasl_plain_username=USERNAME, sasl_plain_password=PASSWORD, sasl_mechanism="PLAIN", security_protocol="SASL_SSL")

topics = []
topics.append(TOPIC)
consumer.subscribe(topics=topics)

for message in consumer:
    message = message.value.decode()
    print(message)

consumer.close()