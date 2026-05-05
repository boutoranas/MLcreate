# CloudML Pipeline — Data Pipeline Scaffold

This repository contains a scaffold for the Data Pipeline portion of the CloudML Pipeline project: Kafka + OpenFaaS + Spark + training orchestration + PostgreSQL + Redis.

What is included:
- OpenFaaS function handlers: `functions/ingest`, `functions/preprocess`, `functions/train`
- PySpark preprocessing job: `spark/spark_job.py`
- Kafka topic creation script: `scripts/create_kafka_topics.py`
- PostgreSQL schema: `db/schema.sql`
- Requirements: `requirements.txt`
- Minimal Dockerfile templates: `dockerfiles/`
- Kubernetes placeholders: `k8s/`

Quick start (local dev flow):
1. Install Python deps: `pip install -r requirements.txt`
2. On Windows, make sure Spark sees JDK 17 and Hadoop helpers:
	- `JAVA_HOME=C:\Program Files\Java\jdk-17`
	- `HADOOP_HOME=C:\hadoop`
	- `PATH` includes `%HADOOP_HOME%\bin`
3. Copy `.env.example` to `.env` if you want a single place for local settings.
4. Start Kafka and Postgres: `docker compose up -d`
5. Create Kafka topics: `python scripts/create_kafka_topics.py`
6. Run the ingest handler locally to simulate upload: `python functions/ingest/handler.py sample-data/sample.csv`

End-to-end local run with the container stack:
1. `docker compose up -d`
2. `python scripts/create_kafka_topics.py`
3. `python functions/ingest/handler.py sample-data/sample.csv`
4. `python functions/preprocess/handler.py messages/<job_id>.json`
5. `python functions/train/handler.py messages/preprocess_<job_id>.json`

Local notes:
- If Kafka is not available, handlers fall back to writing JSON messages into `messages/`.
- The pinned Python stack in `requirements.txt` avoids NumPy/pandas/scikit-learn ABI mismatches on Windows.
- Spark preprocessing requires `winutils.exe` under `C:\hadoop\bin`.
- Kafka listens on `localhost:9092` and Postgres on `localhost:5432` when started via Compose.

See individual folders for details.
