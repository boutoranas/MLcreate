# Kubernetes manifests and Helm charts

This folder will contain Kubernetes manifests and Helm charts to deploy:
- Kafka (or external managed Kafka)
- Redis
- PostgreSQL
- Spark (as either Spark operator or client-submit)
- OpenFaaS functions (ingest, preprocess, train)

For now, use the Dockerfiles in `dockerfiles/` to build images and push them to your registry.
