import kafka
from kafka.admin import KafkaAdminClient, NewTopic

# Define client
kafkaClient = KafkaAdminClient(
    bootstrap_servers="0.0.0.0:9093",
)

# Create topic
topics = []
topics.append(NewTopic(name="lab2", num_partitions=3, replication_factor=1))

try:
    #print()
    kafkaClient.create_topics(new_topics=topics, validate_only=False)
except kafka.errors.TopicAlreadyExistsError:
    print("Topic already created")

# Delete Topic(s)
# kafkaClient.delete_topics(["lab2"])

# List Topics
print("Topics:", kafkaClient.list_topics())
