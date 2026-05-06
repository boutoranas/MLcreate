"""Train handler: trains distributed models via Spark MLlib on processed Parquet.

Usage: python handler.py <preprocess_message.json>
Environment:
- DATABASE_URL for PostgreSQL (sqlalchemy URL)
- Requires Spark cluster or local Spark installation
"""
import os
import sys
import json
from datetime import datetime
import joblib
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, Integer
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

try:
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

try:
    import xgboost as xgb
except Exception:
    xgb = None

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


class MajorityClassifier:
    def __init__(self):
        self.majority_label = None

    def fit(self, X, y):
        counts = y.value_counts()
        self.majority_label = counts.idxmax()
        return self

    def predict(self, X):
        if self.majority_label is None:
            raise ValueError("Model has not been fit yet")
        return [self.majority_label] * len(X)


class MeanRegressor:
    def __init__(self):
        self.mean_value = None

    def fit(self, X, y):
        values = list(y)
        self.mean_value = sum(values) / len(values) if values else 0.0
        return self

    def predict(self, X):
        if self.mean_value is None:
            raise ValueError("Model has not been fit yet")
        return [self.mean_value] * len(X)


def train_model_spark(processed_path, model_out_path, model_type="classification"):
    """Train a distributed model using Spark MLlib."""
    if not SPARK_AVAILABLE:
        raise ImportError("Spark MLlib not available; cannot use distributed training")
    
    spark = SparkSession.builder \
        .appName("cloudml-train") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Read processed Parquet
        df = spark.read.parquet(processed_path)
        print(f"[Train] Loaded parquet with {df.count()} rows")
        
        # Assume 'label' is the target, everything else is features
        feature_cols = [col for col in df.columns if col != 'label']
        print(f"[Train] Using features: {feature_cols}")
        
        # Assemble features into a single vector column
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        df_assembled = assembler.transform(df)
        
        # Train/test split (distributed)
        train_df, test_df = df_assembled.randomSplit([0.8, 0.2], seed=42)
        print(f"[Train] Train set: {train_df.count()}, Test set: {test_df.count()}")
        
        # Train distributed model using XGBoost on Spark
        if model_type == "regression":
            model = SparkXGBRegressor(
                features_col="features",
                label_col="label",
                num_round=100,
                max_depth=6,
                learning_rate=0.1,
                seed=42
            )
            evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="rmse"
            )
            metric_name = "rmse"
        else:
            model = SparkXGBClassifier(
                features_col="features",
                label_col="label",
                num_round=100,
                max_depth=6,
                learning_rate=0.1,
                seed=42
            )
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="accuracy"
            )
            metric_name = "accuracy"
        
        # Fit distributed XGBoost model (happens across workers)
        print(f"[Train] Training {model_type} model with Spark XGBoost distributed across Spark workers...")
        trained_model = model.fit(train_df)
        
        # Evaluate
        predictions = trained_model.transform(test_df)
        n_samples = test_df.count()
        
        if model_type == "regression":
            # Compute all regression metrics
            mse_evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="mse"
            )
            rmse_evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="rmse"
            )
            r2_evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="r2"
            )
            mse_value = mse_evaluator.evaluate(predictions)
            rmse_value = rmse_evaluator.evaluate(predictions)
            r2_value = r2_evaluator.evaluate(predictions)
            metrics = {
                "mse": mse_value,
                "rmse": rmse_value,
                "r2": r2_value,
                "n_samples": n_samples
            }
            print(f"[Train] Distributed model trained. rmse={rmse_value:.4f}, r2={r2_value:.4f}, mse={mse_value:.4f}")
        else:
            # Classification: use single metric
            metric_value = evaluator.evaluate(predictions)
            metrics = {metric_name: metric_value, "n_samples": n_samples}
            print(f"[Train] Distributed model trained. {metric_name}={metric_value:.4f}")
        
        # Save Spark MLlib model
        model_spark_path = model_out_path + ".spark"
        trained_model.write().overwrite().save(model_spark_path)
        print(f"[Train] Saved Spark model to {model_spark_path}")
        
        # Also save metadata
        metadata = {
            "model_type": "spark_mllib",
            "spark_path": model_spark_path,
            "feature_cols": feature_cols,
            "task_type": model_type,
        }
        with open(model_out_path + ".meta", "w") as f:
            json.dump(metadata, f)
        
        return {"model_path": model_out_path, "metrics": metrics, "model_type": model_type}
    
    finally:
        spark.stop()


def compute_metrics(y_true, y_pred, model_type="classification"):
    y_true_list = list(y_true)
    y_pred_list = list(y_pred)
    total = len(y_true_list)
    if model_type == "regression":
        if not total:
            return {"mse": None, "rmse": None, "r2": None, "n_samples": 0}

        squared_errors = [(float(actual) - float(predicted)) ** 2 for actual, predicted in zip(y_true_list, y_pred_list)]
        mse = sum(squared_errors) / total if total else None
        rmse = (mse ** 0.5) if mse is not None else None

        true_mean = sum(float(value) for value in y_true_list) / total
        total_variance = sum((float(value) - true_mean) ** 2 for value in y_true_list)
        residual_variance = sum(squared_errors)
        r2 = 1 - (residual_variance / total_variance) if total_variance else None

        return {
            "mse": mse,
            "rmse": rmse,
            "r2": r2,
            "n_samples": total,
        }

    correct = sum(1 for actual, predicted in zip(y_true_list, y_pred_list) if actual == predicted)
    accuracy = (correct / total) if total else None
    return {
        "accuracy": accuracy,
        "n_samples": total,
    }


def load_parquet_training_data(processed_path):
    try:
        import pandas as pd

        df = pd.read_parquet(processed_path)
        if 'label' not in df.columns:
            raise ValueError('label column not found')
        X = df.drop(columns=['label'])
        y = df['label']
        return X, y
    except Exception:
        import pyarrow.parquet as pq

        table = pq.read_table(processed_path)
        if 'label' not in table.column_names:
            raise ValueError('label column not found')
        data = table.to_pydict()
        feature_names = [name for name in table.column_names if name != 'label']
        X = [list(values) for values in zip(*(data[name] for name in feature_names))] if feature_names else [[] for _ in range(table.num_rows)]
        y = data['label']
        return X, y


def train_model(processed_path, model_out_path, model_type="classification"):
    X, y = load_parquet_training_data(processed_path)
    stratify_y = y if hasattr(y, "nunique") and y.nunique() > 1 else None
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=stratify_y if model_type == "classification" else None,
        )
    except Exception:
        X_train, X_test, y_train, y_test = X, X, y, y

    if model_type == "regression":
        if xgb is not None:
            try:
                clf = xgb.XGBRegressor(objective="reg:squarederror")
                clf.fit(X_train, y_train)
            except Exception as exc:
                print(f"xgboost unavailable at runtime ({exc}); using local linear-regression fallback")
                try:
                    clf = LinearRegression().fit(X_train, y_train)
                except Exception:
                    clf = MeanRegressor().fit(X_train, y_train)
        else:
            try:
                clf = LinearRegression().fit(X_train, y_train)
            except Exception:
                clf = MeanRegressor().fit(X_train, y_train)
    elif xgb is not None:
        try:
            clf = xgb.XGBClassifier(eval_metric='logloss')
            clf.fit(X_train, y_train)
        except Exception as exc:
            print(f"xgboost unavailable at runtime ({exc}); using local majority-class fallback")
            clf = MajorityClassifier().fit(X_train, y_train)
    else:
        clf = MajorityClassifier().fit(X_train, y_train)
    predictions = clf.predict(X_test)
    metrics = compute_metrics(y_test, predictions, model_type=model_type)
    joblib.dump(clf, model_out_path)
    return {"model_path": model_out_path, "metrics": metrics, "model_type": model_type}


def publish_message(message, topic="training_complete"):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    out_dir = os.path.join(os.getcwd(), "messages")
    os.makedirs(out_dir, exist_ok=True)
    
    # Always write to disk for local dev/testing
    msg_file = os.path.join(out_dir, f"train_{message['job_id']}.json")
    with open(msg_file, "w") as f:
        json.dump(message, f, indent=2)
    print(f"Wrote message to {msg_file}")
    
    if KafkaProducer is None:
        return
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        print("Published message to Kafka topic", topic)
    except Exception as exc:
        print(f"Kafka unavailable ({exc}); using local fallback only")


def record_metadata(db_url, job_id, model_path, metrics=None):
    if not db_url:
        print("No DATABASE_URL provided; skipping metadata write")
        return
    engine = create_engine(db_url)
    meta = MetaData()
    models = Table('models', meta,
                   Column('job_id', String, primary_key=True),
                   Column('model_path', String),
                   Column('created_at', String))
    meta.create_all(engine)
    ins = models.insert().values(job_id=job_id, model_path=model_path, created_at=datetime.utcnow().isoformat() + 'Z')
    with engine.connect() as conn:
        conn.execute(ins)
        conn.commit()


def main():
    if len(sys.argv) < 2:
        print("Usage: python handler.py <preprocess_message.json> [classification|regression]")
        sys.exit(1)
    msg_file = sys.argv[1]
    cli_model_type = sys.argv[2] if len(sys.argv) > 2 else None
    with open(msg_file) as f:
        msg = json.load(f)
    processed_path = msg.get('processed_path')
    job_id = msg.get('job_id')
    model_type = (cli_model_type or msg.get('model_type') or msg.get('input_type') or 'classification').lower()
    if model_type not in {'classification', 'regression'}:
        print(f"Unknown model type '{model_type}', defaulting to classification")
        model_type = 'classification'
    models_dir = os.environ.get('MODELS_DIR', os.path.join(os.getcwd(), 'models'))
    os.makedirs(models_dir, exist_ok=True)
    model_out_path = os.path.join(models_dir, f"{job_id}.pkl")
    
    # Try distributed Spark training first; fall back to local if Spark unavailable
    result = None
    used_spark = False
    try:
        if SPARK_AVAILABLE:
            print(f"[Train] Attempting distributed Spark MLlib training for {model_type}...")
            result = train_model_spark(processed_path, model_out_path, model_type=model_type)
            used_spark = True
        else:
            raise ImportError("Spark MLlib not available")
    except Exception as spark_exc:
        print(f"[Train] Spark training unavailable ({spark_exc}); falling back to local training")
        result = train_model(processed_path, model_out_path, model_type=model_type)
    
    record_metadata(os.environ.get('DATABASE_URL'), job_id, model_out_path)
    done_msg = {
        'job_id': job_id,
        'model_path': model_out_path,
        'model_type': model_type,
        'training_mode': 'distributed_spark' if used_spark else 'local',
        'metrics': result.get('metrics') if result else None,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    publish_message(done_msg)


if __name__ == '__main__':
    main()
