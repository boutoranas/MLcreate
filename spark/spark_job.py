"""Simple PySpark preprocessing job: read CSV, basic cleaning, write Parquet.

Usage: python spark_job.py <input_csv> <output_parquet>
"""
import os
import subprocess
import sys


def _looks_like_java_home(java_home):
    return bool(java_home) and os.path.exists(os.path.join(java_home, "bin", "java.exe"))


def _looks_like_hadoop_home(hadoop_home):
    return bool(hadoop_home) and os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe"))


def ensure_java_home():
    java_home = os.environ.get("JAVA_HOME", "")
    preferred_homes = [
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Eclipse Adoptium\jdk-17",
    ]
    if _looks_like_java_home(java_home) and "jdk-17" in java_home.lower():
        return java_home
    for candidate in preferred_homes:
        if _looks_like_java_home(candidate):
            os.environ["JAVA_HOME"] = candidate
            return candidate
    if _looks_like_java_home(java_home):
        return java_home
    try:
        result = subprocess.check_output(
            ["java", "-XshowSettings:properties", "-version"],
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in result.splitlines():
            if "java.home =" in line:
                detected_home = line.split("=", 1)[1].strip()
                if _looks_like_java_home(detected_home):
                    os.environ["JAVA_HOME"] = detected_home
                    return detected_home
    except Exception:
        pass
    return java_home


def ensure_hadoop_home():
    hadoop_home = os.environ.get("HADOOP_HOME", "")
    candidates = [
        r"C:\hadoop",
        r"C:\Program Files\hadoop",
    ]
    if _looks_like_hadoop_home(hadoop_home):
        os.environ.setdefault("hadoop.home.dir", hadoop_home)
        return hadoop_home
    for candidate in candidates:
        if _looks_like_hadoop_home(candidate):
            os.environ["HADOOP_HOME"] = candidate
            os.environ["hadoop.home.dir"] = candidate
            bin_dir = os.path.join(candidate, "bin")
            current_path = os.environ.get("PATH", "")
            if bin_dir not in current_path:
                os.environ["PATH"] = bin_dir + os.pathsep + current_path
            return candidate
    return hadoop_home


ensure_java_home()
ensure_hadoop_home()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def build_spark_session():
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    java_library_path = hadoop_bin.replace("\\", "/")
    java_hadoop_home = hadoop_home.replace("\\", "/")
    java_options = f"-Djava.library.path={java_library_path} -Dhadoop.home.dir={java_hadoop_home}"
    return (
        SparkSession.builder
        .appName("cloudml-preprocess")
        .config("spark.driver.extraLibraryPath", hadoop_bin)
        .config("spark.executor.extraLibraryPath", hadoop_bin)
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.hadoop.io.native.lib.available", "true")
    )


def main():
    if len(sys.argv) < 3:
        print("Usage: python spark_job.py <input_csv> <output_parquet>")
        sys.exit(1)
    input_csv = sys.argv[1]
    output_parquet = sys.argv[2]
    spark = build_spark_session().getOrCreate()
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)
    # Basic cleaning: drop rows where label is null, drop duplicate ids if present
    if 'label' in df.columns:
        df = df.filter(col('label').isNotNull())
    if 'id' in df.columns:
        df = df.dropDuplicates(['id'])
    # TODO: add encoding, imputation, scaling
    df.write.mode('overwrite').parquet(output_parquet)
    print("Wrote processed data to", output_parquet)
    spark.stop()


if __name__ == "__main__":
    main()

