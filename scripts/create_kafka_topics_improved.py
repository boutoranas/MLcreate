#!/usr/bin/env python3
"""
Initialize Kafka topics for the pipeline.
Run this once after starting Kafka, or add it as a service/hook.
"""
import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topics = [
        NewTopic(name="csv_upload_requested", num_partitions=1, replication_factor=1),
        NewTopic(name="dataset_uploaded", num_partitions=1, replication_factor=1),
        NewTopic(name="preprocessing_done", num_partitions=1, replication_factor=1),
        NewTopic(name="training_complete", num_partitions=1, replication_factor=1),
    ]
    
    print(f"Creating Kafka topics on {bootstrap}")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    
    try:
        try:
            admin.create_topics(new_topics=topics, validate_only=False)
            for topic in topics:
                print(f"✓ Requested topic '{topic.name}'")
        except TopicAlreadyExistsError:
            for topic in topics:
                print(f"✓ Topic '{topic.name}' already exists")
    finally:
        admin.close()

if __name__ == "__main__":
    main()
