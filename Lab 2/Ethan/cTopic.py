from kafka.admin import KafkaAdminClient, NewTopic


admin = KafkaAdminClient(
    bootstrap_servers="pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092", 
    client_id='myapp', 
    sasl_plain_username="Z2LCSWDHM7D45WDF", 
    sasl_plain_password="8b0aAKk7RDMAjfgvqJ9Nlf2PGKbUVAp9hMKpyDRZ5IMdkwzXPe3mc1VSdoJLN00G", 
    sasl_mechanism="PLAIN", 
    security_protocol="SASL_SSL"
)

topics = []
topics.append(NewTopic(name='confluent', num_partitions=3, replication_factor=1))
admin.create_topics(new_topics=topics, validate_only=False)
print("Topic Created")
print("Topics:", admin.list_topics())
