import os
import datetime
import findspark
findspark.init()

from pyspark.sql import SparkSession

def log_env():
    print(f"[{datetime.datetime.now()}] Launching Spark...")
    print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")
    print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")

def main():
    log_env()
    spark = SparkSession.builder.appName("DiagnosticLauncher").getOrCreate()
    print(f"Spark version: {spark.version}")
    spark.stop()

if __name__ == "__main__":
    main()