# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, window, to_timestamp

# Création de la session Spark
spark = SparkSession.builder.appName("Seismic Data Analysis").getOrCreate()

# Chargement des données sismiques depuis HDFS
df = spark.read.csv("hdfs://namenode:9000/user/hive/warehouse/seismic_data_ville/dataset_sismique_ville.csv", 
                     header=True, inferSchema=True)

# Conversion du type de colonne date en format Timestamp
df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

# Suppression des valeurs nulles
df_cleaned = df.dropna()

# Suppression des valeurs aberrantes (magnitude > 10)
df_cleaned = df_cleaned.filter(col("magnitude") <= 10)

# Standardisation des valeurs de tension entre plaques (suppression des outliers)
stats = df_cleaned.select(avg(col("tension entre plaque")).alias("moyenne"), 
                          stddev(col("tension entre plaque")).alias("ecart_type")).collect()
moyenne = stats[0]["moyenne"]
ecart_type = stats[0]["ecart_type"]

df_cleaned = df_cleaned.filter(
    (col("tension entre plaque") > (moyenne - 3 * ecart_type)) & 
    (col("tension entre plaque") < (moyenne + 3 * ecart_type))
)

print("Données nettoyées avec succès !")

# Calcul de l'amplitude à partir de la magnitude (approximation avec log10)
df_analyse = df_cleaned.withColumn("amplitude", 10**(col("magnitude") - 3))


# 1. Vérification de la corrélation entre magnitude et tension entre plaques
correlation_magnitude_tension = df_cleaned.stat.corr("magnitude", "tension entre plaque")
print("Corrélation entre Magnitude et Tension entre plaques :", correlation_magnitude_tension)


# 2. Identifier les périodes d'activité sismique forte (magnitude > 5)
df_activite_forte = df_analyse.filter(col("magnitude") > 5)
print(f"Nombre d'événements sismiques forts (magnitude > 5) : {df_activite_forte.count()}")


# 3. Regrouper les événements par heure et calculer la magnitude moyenne
df_agreg = df_cleaned.groupBy(window("date", "1 hour")).agg(avg("magnitude").alias("magnitude_moyenne"))
print("Regroupement des événements par heure et calcul de la magnitude moyenne")
df_agreg.show(5)




# # Cela sauvegarde les résultats analysés (avec amplitude) dans un fichier CSV
# df_analyse.write.csv("/path/to/seismic_analysis_result.csv", header=True) 


# Retourner les résultats sous forme de DataFrame pour utilisation ultérieure
df_analyse.show(5)
