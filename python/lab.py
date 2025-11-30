from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, RDD

def run(spark: SparkSession, sc: SparkContext, df: DataFrame, rdd: RDD):
    """
    This function is called automatically by the server when you save this file.
    
    Available objects:
    - spark: SparkSession
    - sc: SparkContext
    - df: DataFrame (loaded from books_ratings.csv)
    - rdd: RDD (loaded from books_ratings.csv)
    """
    print("-" * 50)
    print("Executing lab.py...")
    print("-" * 50)

    # --- YOUR CODE HERE ---
    
    # Example 1: Count rows in DataFrame
    print(f"DataFrame Count: {df.count()}")

    # Example 2: Count rows in RDD
    print(f"RDD Count: {rdd.count()}")
    
    # Example 3: Filter RDD (excluding header)
    header = rdd.first()
    data_rdd = rdd.filter(lambda line: line != header)
    print(f"Data Rows (RDD): {data_rdd.count()}")

    print("-" * 50)
    print("Execution finished.")
    print("-" * 50)