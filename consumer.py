from hdfs import InsecureClient
import pandas as pd
import json
import time
from kafka import KafkaProducer
from io import StringIO

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Connexion à HDFS
client = InsecureClient('http://namenode:9000', user='hadoop')

# Chemin du fichier sur HDFS
hdfs_path = "/user/hive/warehouse/seismic_data_ville/dataset_sismique_ville.csv"

# Lire le fichier directement depuis HDFS
try:
    with client.read(hdfs_path) as reader:
        data = reader.read().decode('utf-8')
    print("Fichier lu avec succès depuis HDFS.")
except Exception as e:
    print(f"Erreur lors de la lecture du fichier HDFS : {e}")
    exit(1)

# Convertir les données en DataFrame Pandas
df = pd.read_csv(StringIO(data))

# Envoyer chaque ligne à Kafka
for _, row in df.iterrows():
    event = {
        "date": row["date"],
        "ville": row["ville"],  
        "secousse": row["secousse"],
        "magnitude": row["magnitude"],
        "tension_entre_plaque": row["tension entre plaque"]
    }
    producer.send("seismic-events", value=event)
    print("Envoyé :" , event)
    time.sleep(1)  # ,  Simule un flux temps réel

print("Toutes les données ont été envoyées à Kafka !")
