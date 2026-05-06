FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

WORKDIR /workspace

RUN pip install --upgrade pip && pip install scikit-learn==1.2.2 sqlalchemy==2.0.19 psycopg2-binary==2.9.6 joblib==1.2.0 pandas==2.0.3 pyarrow==15.0.2 kafka-python==2.0.2

COPY . ./

ENTRYPOINT ["python", "consumers/train_consumer.py"]
