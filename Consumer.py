import os
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from decouple import config
import json
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Función para obtener y validar una variable de entorno
def get_env_var(key, default=None):
    value = config(key, default=default)
    if value is None:
        logging.error(f"Environment variable {key} is not set.")
    return value

# Configuración del Consumer de Kafka
conf = {
    'bootstrap.servers': get_env_var('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),  
    'group.id': get_env_var('KAFKA_GROUP_ID', 'riot-games-group'),
    'client.id': get_env_var('KAFKA_CLIENT_ID', 'riot-games-consumer')
}

consumer = Consumer(conf)

# Suscribirse al tema 'riot-games-topic'
consumer.subscribe(['riot-games-topic'])

# Función para inicializar la conexión a MongoDB
def iniciar_mongo():
    mongo_username = get_env_var('MONGO_USERNAME')  # Obtener el nombre de usuario de MongoDB desde las variables de entorno
    mongo_password = get_env_var('MONGO_PASSWORD')  # Obtener la contraseña de MongoDB desde las variables de entorno
    mongo_host = get_env_var('MONGO_HOST', 'mongo')  # Nombre del servicio de MongoDB en Docker Compose
    mongo_port = get_env_var('MONGO_PORT', '27017')  # Puerto de MongoDB 
    mongo_url = f'mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/'

    # Conectar a MongoDB y seleccionar la base de datos y colección
    client = MongoClient(mongo_url)
    db = client[get_env_var('MONGO_DB_NAME', 'riot_games_db')]
    collection = db[get_env_var('MONGO_COLLECTION_NAME', 'matches')]
    return collection

# Inicializar la colección de MongoDB
collection = iniciar_mongo()

def consumir_mensajes():
    try:
        while True:
            # Esperar a que lleguen nuevos mensajes
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partición (alcanzamos el final del tema)
                    continue
                else:
                    # Error inesperado
                    logging.error(f"Error al consumir el mensaje: {msg.error()}")
                    break

            # Procesar el mensaje recibido
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')
            logging.info(f"Key: {key}, Value: {value}")

            # Almacenar el mensaje en MongoDB
            document = {
                'key': key,
                'value': json.loads(value)  # Deserializar el JSON antes de almacenarlo
            }
            collection.insert_one(document)
            logging.info(f"Mensaje con Key: {key} almacenado en MongoDB")

    except KeyboardInterrupt:
        # Manejar la interrupción del teclado (Ctrl+C)
        logging.info("Interrupción del teclado recibida. Cerrando el consumidor...")
    finally:
        # Cerrar el consumidor
        consumer.close()
        logging.info("Consumidor cerrado")

if __name__ == "__main__":
    consumir_mensajes()
