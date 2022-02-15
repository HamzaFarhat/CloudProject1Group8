from kafka import KafkaProducer
import sys

try:
    producer = KafkaProducer(bootstrap_servers='pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092')
    if len(sys.argv) == 2:
        producer.send('Demo1', sys.argv[1].encode())
        print("Message sent: " + sys.argv[1] )
        producer.flush()
        producer.close()
    else:
        print("Please enter a message")

except:
    print("Soemthing went wrong")