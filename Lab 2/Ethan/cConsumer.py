from kafka import KafkaConsumer

consumer = KafkaConsumer('poems', bootstrap_servers='pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092', 
sasl_plain_username="Z2LCSWDHM7D45WDF", 
sasl_plain_password="8b0aAKk7RDMAjfgvqJ9Nlf2PGKbUVAp9hMKpyDRZ5IMdkwzXPe3mc1VSdoJLN00G", 
sasl_mechanism="PLAIN", 
security_protocol="SASL_SSL")
for msg in consumer:
    print (msg.value.decode())