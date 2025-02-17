from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, window, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType

# Création de la session Spark avec support Streaming
spark = SparkSession.builder \
    .appName("Seismic Data Streaming Analysis") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Définition du schéma des données Kafka
schema = StructType([
    StructField("date", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("secousse", BooleanType(), True),
    StructField("magnitude", FloatType(), True),
    StructField("tension_entre_plaque", FloatType(), True)
])

# Lecture du topic Kafka en streaming
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "seismic-events") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion des données JSON en DataFrame
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Conversion de la colonne date
df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

# Suppression des valeurs nulles
df_cleaned = df.dropna()

# uppression des valeurs aberrantes (magnitude > 10)
df_cleaned = df_cleaned.filter(col("magnitude") <= 10)

# Standardisation des valeurs de tension entre plaques (filtrage des outliers)
stats = df_cleaned.select(avg(col("tension_entre_plaque")).alias("moyenne"),
                          stddev(col("tension_entre_plaque")).alias("ecart_type"))

moyenne = stats.collect()[0]["moyenne"]
ecart_type = stats.collect()[0]["ecart_type"]

df_cleaned = df_cleaned.filter(
    (col("tension_entre_plaque") > (moyenne - 3 * ecart_type)) & 
    (col("tension_entre_plaque") < (moyenne + 3 * ecart_type))
)

# Calcul de l'amplitude à partir de la magnitude
df_analyse = df_cleaned.withColumn("amplitude", 10**(col("magnitude") - 3))

# Vérification de la corrélation entre magnitude et tension entre plaques
df_correlation = df_cleaned.select("magnitude", "tension_entre_plaque")

# Détection des périodes d'activité sismique forte (magnitude > 5)
df_activite_forte = df_analyse.filter(col("magnitude") > 5)

# Agrégation : Regrouper les événements par heure et calculer la magnitude moyenne
df_agreg = df_cleaned.groupBy(window("date", "1 hour")).agg(avg("magnitude").alias("magnitude_moyenne"))

# Écriture des résultats en console (peut être remplacé par un stockage HDFS)
query_activite_forte = df_activite_forte.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_agreg = df_agreg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query_correlation = df_correlation.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_activite_forte.awaitTermination()
query_agreg.awaitTermination()
query_correlation.awaitTermination()
