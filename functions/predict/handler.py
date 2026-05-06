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
    import pandas as pd
    SPARK_AVAILABLE = True
except Exception as e:
    SPARK_AVAILABLE = False

import re


def _fix_metadata_nan(metadata_path):
    """Fix NaN values in Spark metadata JSON for Jackson parsing."""
    try:
        with open(metadata_path, 'r') as f:
            content = f.read()
        # Replace NaN and Infinity with null (JSON-compatible values)
        fixed_content = re.sub(r':NaN\b', ':null', content)
        fixed_content = re.sub(r':Infinity\b', ':null', fixed_content)
        fixed_content = re.sub(r':-Infinity\b', ':null', fixed_content)
        # Write back temporarily
        with open(metadata_path, 'w') as f:
            f.write(fixed_content)
        return True
    except Exception as e:
        print(f"[Predict] Warning: Could not fix metadata JSON: {e}")
        return False


def predict_with_spark(csv_path, spark_model_dir, model_type="classification"):
    """Load Spark model and make predictions on CSV data."""
    if not SPARK_AVAILABLE:
        raise ImportError("Spark not available; cannot use Spark model for prediction")
    
    import sys
    python_exec = sys.executable
    
    spark = SparkSession.builder \
        .appName("cloudml-predict") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.pyspark.python", python_exec) \
        .config("spark.pyspark.driver.python", python_exec) \
        .config("spark.driver.maxResultSize", "1g") \
        .getOrCreate()
    
    try:
        # Load the Spark model from directory
        print(f"[Predict] Loading Spark model from {spark_model_dir}")
        model = PipelineModel.load(spark_model_dir)
        
        # Read input CSV as Spark DataFrame
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print(f"[Predict] Loaded CSV with {df.count()} rows and columns: {df.columns}")
        
        # Make predictions
        predictions = model.transform(df)
        print(f"[Predict] Made predictions on {predictions.count()} samples")
        
        # Write predictions to CSV output file
        predictions_dir = os.environ.get('PREDICTIONS_OUTPUT_DIR', os.path.join(os.getcwd(), 'data', 'predictions_output'))
        os.makedirs(predictions_dir, exist_ok=True)
        output_csv = os.path.join(predictions_dir, f"predictions_{datetime.now().strftime('%s')}.csv")
        predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"[Predict] Wrote predictions to {output_csv}")
        
        # Collect results: keep original features and predictions
        result_df = predictions.select("*")
        rows = result_df.collect()
        
        # Convert to list of dicts for JSON serialization
        results = []
        for row in rows:
            row_dict = row.asDict()
            results.append(row_dict)
        
        # Get column names
        columns = df.columns + ["prediction"]
        
        return {
            "predictions": results,
            "n_rows": len(results),
            "columns": columns,
            "model_type": model_type,
            "prediction_mode": "spark",
            "output_csv": output_csv
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
    output_csv = os.path.join(predictions_dir, f"predictions_{datetime.now().strftime('%s')}.csv")
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
        
        result_file = os.path.join(out_dir, f"predict_{model_id}_{datetime.now().strftime('%s')}.json")
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
    main()
