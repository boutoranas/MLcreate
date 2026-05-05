"""Create Kafka topics used by the pipeline (local helper).

This script uses kafka-python and expects `KAFKA_BOOTSTRAP` env var or defaults to localhost:9092.
"""
import os
from kafka.admin import KafkaAdminClient, NewTopic

TOPICS = [
    ("dataset_uploaded", 1),
    ("preprocessing_done", 1),
    ("training_complete", 1),
]


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="cloudml-admin")
    topics = [NewTopic(name=t[0], num_partitions=t[1], replication_factor=1) for t in TOPICS]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print("Topics created:", [t[0] for t in TOPICS])
    except Exception as e:
        print("Create topics warning/error:", e)


if __name__ == "__main__":
    main()
