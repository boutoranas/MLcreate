FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-17-jre-headless \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /workspace

RUN pip install --upgrade pip && pip install pyspark==3.4.1 pandas==2.0.3 pyarrow==15.0.2 boto3==1.34.0

COPY . ./

ENTRYPOINT ["python", "consumers/preprocess_consumer.py"]
