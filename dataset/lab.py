from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, RDD
from pyspark.sql.functions import split, col

def run(spark: SparkSession, sc: SparkContext, df: DataFrame, rdd: RDD):
    """
    This function is called automatically by the server when you save this file.
    
    Available objects:
    - spark: SparkSession
    - sc: SparkContext
    - df: DataFrame (loaded from books_ratings.csv)
    - rdd: RDD (loaded from books_ratings.csv)
    """

    # ================================
    # 1) DataFrame original
    # ================================
    print("\n[1] DataFrame ORIGINAL")
    print("Cantidad de filas  :", df.count())
    print("Cantidad de columnas:", len(df.columns))

    # ================================
    # 2) DataFrame con columnas reales (split por ;)
    # ================================
    print("\n[2] DataFrame RECONSTRUIDO (10 columnas)")

    # Renombramos la única columna a algo seguro
    raw_col = df.columns[0]
    df2 = df.withColumnRenamed(raw_col, "raw")

    # Split básico
    df2 = df2.withColumn("parts", split(col("raw"), ";"))

    # Cantidad de columnas detectadas
    num_cols = len(df2.select("parts").first()[0])

    # Creamos select dinámico: col0, col1, ...
    cols = [col("parts").getItem(i).alias(f"col{i}") for i in range(num_cols)]

    df_final = df2.select(*cols)

    print("Cantidad de filas  :", df_final.count())
    print("Cantidad de columnas:", len(df_final.columns))

    # Registrar en SQL
    df_final.createOrReplaceTempView("tabla")

    # ================================
    # 3) SQL
    # ================================
    print("\n[3] SQL (sobre DataFrame reconstruido)")

    print("Cantidad de columnas (SQL):", len(df_final.columns))

    print("Cantidad de filas (SQL):")
    spark.sql("SELECT COUNT(*) AS total FROM tabla").show()

    # ================================
    # 4) RDD
    # ================================
    print("\n[4] RDD")
    print("Cantidad de líneas:", rdd.count())

    first_line = rdd.first()
    num_cols_rdd = len(first_line.split(";"))
    print("Cantidad de columnas detectadas en RDD:", num_cols_rdd)
