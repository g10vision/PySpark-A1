import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("DataFrame Example") \
        .getOrCreate()

    # Sample data: list of tuples
    data = [
        ("Alice", 34),
        ("Bob", 45),
        ("Cathy", 29),
        ("David", 40)
    ]

    # Define schema
    columns = ["name", "age"]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=columns)

    # Filter: age > 30
    df_filtered = df.filter(col("age") > 30)

    # Add new column: age group
    df_enriched = df_filtered.withColumn(
        "age_group",
        col("age") >= 40
    )

    # Show results
    df_enriched.show()

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()