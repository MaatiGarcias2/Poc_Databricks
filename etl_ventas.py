# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def crear_sesion():
    return SparkSession.builder \
        .appName("PipelineVentasEcommerce") \
        .getOrCreate()

# Prueba de conexión rápida
spark = crear_sesion()
print("¡Hola desde el cluster de Databricks! 👋")
spark.range(10).show()

# %%
# Listar los archivos de ejemplo en Databricks
archivos = dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/sales_orders/")
for archivo in archivos:
    print(f"Encontrado: {archivo.path}")
# %%
# Definimos la ruta a la carpeta que contiene los JSON
ruta_json = "dbfs:/databricks-datasets/retail-org/sales_orders/"

# Leemos los datos activando la inferencia de esquema
df_ventas = spark.read.format("json") \
    .option("inferSchema", "true") \
    .load(ruta_json)

# Mostramos la estructura que Spark "adivinó"
df_ventas.printSchema()

# Vemos los primeros 5 registros
df_ventas.show(5)# COMMAND ----------

# COMMAND ----------
