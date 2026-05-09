# Data Contracts & CSV Schema

CSV expectations (simple tabular dataset):
- First row: header with column names
- The user selects the target column at upload time. Remaining columns are treated as features.
- Missing values: represented as empty cells; preprocessing will impute or drop.

Example schema (CSV header):
```
id,feature_1,feature_2,feature_3,label
```

Kafka message formats (JSON):

- `dataset_uploaded` topic message:
```
{
  "job_id": "uuid",
  "uploader": "user@example.com",
  "csv_path": "s3://bucket/path/to/file.csv" or "/local/path/file.csv",
  "target_column": "price",
  "timestamp": "ISO8601"
}
```

- `preprocessing_done` message:
```
{
  "job_id": "uuid",
  "processed_path": "/path/to/processed.parquet",
  "n_rows": 12345,
  "features": ["feature_1","feature_2"],
  "target_column": "price",
  "timestamp": "ISO8601"
}
```

- `training_complete` message:
```
{
  "job_id": "uuid",
  "model_path": "/models/jobid/model.pkl",
  "metrics": {"accuracy":0.95},
  "timestamp": "ISO8601"
}
```
