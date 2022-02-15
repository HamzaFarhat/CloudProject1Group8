from kafka import KafkaConsumer

consumer = KafkaConsumer('Demo1', bootstrap_servers='localhost:9093')
for msg in consumer:
    print (msg.value.decode())