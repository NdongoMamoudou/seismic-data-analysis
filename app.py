import os
from kafka import KafkaProducer, KafkaConsumer

import time
time.sleep(10)

# Récupère le serveur Kafka depuis la variable d'environnement ou utilise une valeur par défaut
kafka_server = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Création d'un producteur Kafka
producer = KafkaProducer(bootstrap_servers=kafka_server)



# Envoi d'un message sur le topic "test"
producer.send('test', b'Hello, Kafka!')
producer.flush()

print("Message envoyé!")

# Création d'un consommateur Kafka pour lire les messages du topic "test"
consumer = KafkaConsumer(
    'test',
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',
    consumer_timeout_ms=3000  # Arrête le consommateur après 3 secondes s'il n'y a plus de messages
)

print("Lecture des messages:")
for msg in consumer:
    print(msg.value.decode('utf-8'))