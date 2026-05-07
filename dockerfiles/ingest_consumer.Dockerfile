FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

WORKDIR /workspace

RUN pip install --upgrade pip && pip install boto3==1.34.0

COPY . ./

ENTRYPOINT ["python", "consumers/ingest_consumer.py"]
