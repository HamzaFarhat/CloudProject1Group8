from kafka.admin import KafkaAdminClient, NewTopic

try:
    admin = KafkaAdminClient(
        bootstrap_servers="localhost:9093", 
        client_id='myapp'
    )

    topics = []
    topics.append(NewTopic(name='Demo1', num_partitions=3, replication_factor=1))
    admin.create_topics(new_topics=topics, validate_only=False)
    print("Topic Created")
    print("Topics:", admin.list_topics())
except:
    print("Soemthing went wrong")    
