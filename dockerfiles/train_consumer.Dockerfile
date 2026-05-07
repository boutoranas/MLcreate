FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1 \
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
	PATH=$PATH:/usr/lib/jvm/java-17-openjdk-amd64/bin

WORKDIR /workspace

# Install Java for Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
	openjdk-17-jdk-headless \
	&& rm -rf /var/lib/apt/lists/*

# Install Python dependencies including PySpark XGBoost for distributed training.
# The extra retries/timeouts help avoid rebuilding from scratch on transient network drops.
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
	numpy==1.24.3 \
	pyarrow==12.0.1 \
	boto3==1.34.0

COPY . ./

ENTRYPOINT ["python", "consumers/train_consumer.py"]
