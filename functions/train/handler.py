"""Train handler: trains a simple XGBoost model on processed Parquet and saves artifact.

Usage: python handler.py <preprocess_message.json>
Environment:
- DATABASE_URL for PostgreSQL (sqlalchemy URL)
"""
import os
import sys
import json
from datetime import datetime
import joblib
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, Integer
from sklearn.model_selection import train_test_split

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


def compute_metrics(y_true, y_pred):
    y_true_list = list(y_true)
    y_pred_list = list(y_pred)
    total = len(y_true_list)
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


def train_model(processed_path, model_out_path):
    X, y = load_parquet_training_data(processed_path)
    stratify_y = y if hasattr(y, "nunique") and y.nunique() > 1 else None
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=stratify_y,
        )
    except Exception:
        X_train, X_test, y_train, y_test = X, X, y, y
    if xgb is not None:
        try:
            clf = xgb.XGBClassifier(eval_metric='logloss')
            clf.fit(X_train, y_train)
        except Exception as exc:
            print(f"xgboost unavailable at runtime ({exc}); using local majority-class fallback")
            clf = MajorityClassifier().fit(X_train, y_train)
    else:
        clf = MajorityClassifier().fit(X_train, y_train)
    predictions = clf.predict(X_test)
    metrics = compute_metrics(y_test, predictions)
    joblib.dump(clf, model_out_path)
    return {"model_path": model_out_path, "metrics": metrics}


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
        print("Usage: python handler.py <preprocess_message.json>")
        sys.exit(1)
    msg_file = sys.argv[1]
    with open(msg_file) as f:
        msg = json.load(f)
    processed_path = msg.get('processed_path')
    job_id = msg.get('job_id')
    models_dir = os.environ.get('MODELS_DIR', os.path.join(os.getcwd(), 'models'))
    os.makedirs(models_dir, exist_ok=True)
    model_out_path = os.path.join(models_dir, f"{job_id}.pkl")
    result = train_model(processed_path, model_out_path)
    record_metadata(os.environ.get('DATABASE_URL'), job_id, model_out_path)
    done_msg = {
        'job_id': job_id,
        'model_path': model_out_path,
        'metrics': result.get('metrics') if result else None,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    publish_message(done_msg)


if __name__ == '__main__':
    main()
