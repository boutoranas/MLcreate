FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

WORKDIR /workspace

RUN pip install --upgrade pip && pip install kafka-python==2.0.2

COPY . ./

ENTRYPOINT ["python", "consumers/ingest_consumer.py"]
