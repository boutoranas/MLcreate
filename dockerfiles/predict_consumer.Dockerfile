FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1 \
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
	PATH=$PATH:/usr/lib/jvm/java-17-openjdk-amd64/bin

WORKDIR /workspace

# Install Java for Spark (needed for loading Spark MLlib models)
RUN apt-get update && apt-get install -y --no-install-recommends \
	openjdk-17-jdk-headless \
	&& rm -rf /var/lib/apt/lists/*

# Install Python dependencies including PySpark for model inference.
RUN pip install --upgrade pip && pip install \
	--retries 10 \
	--timeout 120 \
	--prefer-binary \
	pyspark==3.4.1 \
	xgboost==1.7.6 \
	scikit-learn==1.2.2 \
	sqlalchemy==2.0.19 \
	psycopg2-binary==2.9.6 \
	joblib==1.2.0 \
	pandas==2.0.3 \
	pyarrow==15.0.2 \
	kafka-python==2.0.2 \
	numpy==1.24.3 \
	scipy==1.11.4

COPY . ./

ENTRYPOINT ["python", "consumers/predict_consumer.py"]
