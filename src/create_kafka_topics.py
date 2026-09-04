from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_SERVER = "localhost:9092"
TOPICS = ["listening-events", "catalog-releases"]

def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    existing = admin.list_topics()
    new_topics = [
        NewTopic(name=t, num_partitions=2, replication_factor=1)
        for t in TOPICS if t not in existing
    ]
    if new_topics:
        admin.create_topics(new_topics)
        print(f"Created: {[t.name for t in new_topics]}")
    else:
        print("Topics already exist.")
    admin.close()

if __name__ == "__main__":
    create_topics()
