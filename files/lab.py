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
    
    # # Example 1: Count rows in DataFrame
    # print(f"DataFrame Count: {df.count()}")

    # # Example 2: Count rows in RDD
    # print(f"RDD Count: {rdd.count()}")
    
    # # # Example 3: Filter RDD (excluding header)
    # # header = rdd.first()
    # # data_rdd = rdd.filter(lambda line: line != header)
    # # print(f"Data Rows (RDD): {data_rdd.count()}")

    # print("-" * 50)
    # print("Execution finished.")
    # print("-" * 50)

    # Recordar que la estructura del .csv es:
    # (id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen,fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero)

    def mapeo(linea):
        campos = linea.split(';')
        # clave: id_usuario (campos[-3])
        # valor: (duracion_recorrido, 1) para luego acumular
        return (campos[-3], (float(campos[1]), 1))

    def categoria(l):
        duracion_total, viajes = l[1]
        promedio = duracion_total / viajes
        if viajes > 30 and promedio < 25:
            return ('operativos', 1)
        elif viajes > 5 and promedio > 40:
            return ('deportistas', 1)
        return ('no clasificados', 1)

    a = rdd.map(mapeo)
    b = a.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    c = b.map(categoria)
    d = c.reduceByKey(lambda x, y: x + y)

    print('a')
    print(a.take(5))
    print('b')
    print(b.take(5))
    print('c')
    print(c.take(5))
    print('d')
    print(d.collect())