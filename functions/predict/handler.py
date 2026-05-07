"""Predict handler: loads trained Spark/joblib models and makes predictions on CSV data.

Usage: python handler.py <csv_path> <model_id> [regression|classification]
Environment:
- MODELS_DIR: directory containing model files (default: ./models)
"""
import os
import sys
import json
from datetime import datetime
import joblib
import csv

try:
    from pyspark.sql import SparkSession
    from pyspark.ml import PipelineModel
    from pyspark.ml.feature import VectorAssembler
    import pandas as pd
    SPARK_AVAILABLE = True
except Exception as e:
    SPARK_AVAILABLE = False
    print(f"[Predict] WARNING: Spark unavailable: {e}")

import re



def predict_with_spark(csv_path, spark_model_dir, model_type="classification"):
    if not SPARK_AVAILABLE:
        raise ImportError("Spark not available")

    import sys
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    spark = SparkSession.builder \
        .appName("cloudml-predict") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.pyspark.python", python_exec) \
        .config("spark.pyspark.driver.python", python_exec) \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()
        
    try:
        # Delete stale Hadoop CRC files so LocalFileSystem doesn't reject cross-platform checksums
        for root, _, files in os.walk(spark_model_dir):
            for f in files:
                if f.endswith('.crc') or (f.startswith('.') and f.endswith('.crc')):
                    os.remove(os.path.join(root, f))

        # Read metadata to find the actual saved class.
        # Use regex instead of json.load because the file may contain NaN (invalid JSON
        # for Python, but valid for Spark's Jackson which XGBoost Spark relies on).
        metadata_file = os.path.join(spark_model_dir, "metadata", "part-00000")
        with open(metadata_file, "r") as f:
            metadata_content = f.read()

        # Repair damage from a previous fix that turned XGBoost's "missing":NaN into "missing":null,
        # which XGBoost rejects at predict time. NaN is the XGBoost default and what Jackson expects.
        if re.search(r'"missing"\s*:\s*null', metadata_content):
            metadata_content = re.sub(r'"missing"\s*:\s*null', '"missing":NaN', metadata_content)
            with open(metadata_file, "w") as f:
                f.write(metadata_content)
            print("[Predict] Restored missing=NaN in metadata")

        class_match = re.search(r'"class"\s*:\s*"([^"]+)"', metadata_content)
        class_name = class_match.group(1) if class_match else ""
        print(f"[Predict] Model class: {class_name}")

        # Load using the correct class
        if "PipelineModel" in class_name:
            model = PipelineModel.load(spark_model_dir)
        elif "SparkXGBRegressor" in class_name:
            from xgboost.spark import SparkXGBRegressorModel
            model = SparkXGBRegressorModel.load(spark_model_dir)
        elif "SparkXGBClassifier" in class_name:
            from xgboost.spark import SparkXGBClassifierModel
            model = SparkXGBClassifierModel.load(spark_model_dir)
        else:
            raise ValueError(f"Unsupported Spark model class: {class_name}")

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print(f"[Predict] Loaded CSV with {df.count()} rows")
        
        ignore_cols = {"label", "target", "prediction", "id"}
        feature_cols = [c for c in df.columns if c.lower() not in ignore_cols]
        print(f"[Predict] Assembling features from columns: {feature_cols}")

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)

        predictions = model.transform(df)
        pred_pd = predictions.toPandas()

        drop_cols = [c for c in pred_pd.columns if c in {"features", "rawPrediction", "probability"}]
        pred_pd = pred_pd.drop(columns=drop_cols)

        predictions_dir = os.environ.get('PREDICTIONS_OUTPUT_DIR', os.path.join(os.getcwd(), 'data', 'predictions_output'))
        os.makedirs(predictions_dir, exist_ok=True)
        ts = int(datetime.now().timestamp())
        output_csv = os.path.join(predictions_dir, f"predictions_{ts}.csv")
        pred_pd.to_csv(output_csv, index=False)
        print(f"[Predict] Wrote predictions to {output_csv}")

        return {
            "predictions": pred_pd.to_dict(orient='records'),
            "n_rows": len(pred_pd),
            "columns": list(pred_pd.columns),
            "model_type": model_type,
            "prediction_mode": "spark",
            "output_csv": output_csv,
        }

    finally:
        spark.stop()


def predict_with_joblib(csv_path, model_path, model_type="classification"):
    """Load joblib model and make predictions on CSV data."""
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas required for joblib prediction")
    
    # Load the sklearn/xgboost model
    print(f"[Predict] Loading joblib model from {model_path}")
    model = joblib.load(model_path)
    
    # Read CSV
    df = pd.read_parquet(csv_path) if csv_path.endswith('.parquet') else pd.read_csv(csv_path)
    print(f"[Predict] Loaded CSV with {len(df)} rows and columns: {list(df.columns)}")
    
    # Make predictions
    predictions = model.predict(df)
    print(f"[Predict] Made predictions on {len(predictions)} samples")
    
    # Add predictions column to dataframe
    df['prediction'] = predictions
    
    # Write predictions to CSV output file
    predictions_dir = os.environ.get('PREDICTIONS_OUTPUT_DIR', os.path.join(os.getcwd(), 'data', 'predictions_output'))
    os.makedirs(predictions_dir, exist_ok=True)
    output_csv = os.path.join(predictions_dir, f"predictions_{int(datetime.now().timestamp())}.csv")
    df.to_csv(output_csv, index=False)
    print(f"[Predict] Wrote predictions to {output_csv}")
    
    # Convert results to list of dicts
    results = df.to_dict(orient='records')
    
    return {
        "predictions": results,
        "n_rows": len(results),
        "columns": list(df.columns),
        "model_type": model_type,
        "prediction_mode": "joblib",
        "output_csv": output_csv
    }


def load_model_metadata(model_path):
    """Load model metadata."""
    metadata_path = model_path + ".meta"
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as f:
            return json.load(f)
    return {}


def predict(csv_path, model_id, model_type="classification"):
    """Main predict function."""
    models_dir = os.environ.get('MODELS_DIR', os.path.join(os.getcwd(), 'models'))
    
    # Strip file extensions if user provided them
    clean_model_id = model_id.replace(".pkl.spark", "").replace(".pkl", "")
    
    # Check for Spark model directory first
    spark_model_path = os.path.join(models_dir, f"{clean_model_id}.pkl.spark")
    joblib_model_path = os.path.join(models_dir, f"{clean_model_id}.pkl")
    metadata_path = os.path.join(models_dir, f"{clean_model_id}.pkl.meta")
    
    # Try to load metadata to determine model type and format
    metadata = {}
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
    
    saved_model_type = metadata.get("task_type", model_type)
    
    # Try Spark first (if directory exists), then fall back to joblib
    result = None
    used_spark = False
    
    if SPARK_AVAILABLE and os.path.isdir(spark_model_path):
        print(f"[Predict] Found Spark model at {spark_model_path}")
        try:
            result = predict_with_spark(csv_path, spark_model_path, model_type=saved_model_type)
            used_spark = True
        except Exception as spark_exc:
            print(f"[Predict] Spark prediction failed: {spark_exc}; falling back to joblib")
            if os.path.exists(joblib_model_path):
                print(f"[Predict] Using joblib model at {joblib_model_path}")
                result = predict_with_joblib(csv_path, joblib_model_path, model_type=saved_model_type)
            else:
                raise
    elif os.path.exists(joblib_model_path):
        print(f"[Predict] Found joblib model at {joblib_model_path}")
        result = predict_with_joblib(csv_path, joblib_model_path, model_type=saved_model_type)
    else:
        raise FileNotFoundError(f"No model found for ID {clean_model_id}. Checked {spark_model_path} and {joblib_model_path}")
    
    return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python handler.py <csv_path> <model_id> [classification|regression]")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    model_id = sys.argv[2]
    model_type = sys.argv[3] if len(sys.argv) > 3 else "classification"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
        result = predict(csv_path, model_id, model_type)
        
        # Write results to messages directory
        out_dir = os.path.join(os.getcwd(), "messages")
        os.makedirs(out_dir, exist_ok=True)
        
        result_msg = {
            "model_id": model_id,
            "csv_path": csv_path,
            "model_type": model_type,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "output_csv": result.get("output_csv"),
            "result": {
                "n_rows": result.get("n_rows"),
                "columns": result.get("columns"),
                "prediction_mode": result.get("prediction_mode"),
                "sample_predictions": result.get("predictions", [])[:5]  # First 5 for logging
            }
        }
        
        result_file = os.path.join(out_dir, f"predict_{model_id}_{int(datetime.now().timestamp())}.json")
        with open(result_file, "w") as f:
            json.dump(result_msg, f, indent=2)
        print(f"Wrote results to {result_file}")
        
        # Print full results as JSON to stdout for API to capture
        print("\n=== PREDICTION_RESULTS_START ===")
        print(json.dumps(result, indent=2, default=str))
        print("=== PREDICTION_RESULTS_END ===")
        
    except Exception as e:
        print(f"Prediction failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    print(f"[Predict] SPARK_AVAILABLE: {SPARK_AVAILABLE}")
    main()
