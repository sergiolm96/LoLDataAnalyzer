import os
import pandas as pd
import requests
import time
import json
from collections import deque
from dotenv import load_dotenv
from confluent_kafka import Producer

# Cargar variables de entorno desde un archivo .env
load_dotenv()

# URL base de la API de Riot Games y clave de API
BASE_URL = os.getenv('BASE_URL')    #Usar variable de entorno para la url base
API_KEY = os.getenv('RIOT_API_KEY')  # Usar variable de entorno para la clave de API

# Cantidad de partidas que buscamos de cada jugador y límite total de partidas
no_games = 100
limite = 5000

# Configuración del Producer de Kafka
conf = {
    'bootstrap.servers': 'kafka:9092',  
    'client.id': 'riot-games-producer'
}
producer = Producer(conf)

# Definir la cola para almacenar las marcas de tiempo de las solicitudes
request_times = deque(maxlen=100)

# Función para gestionar el límite de peticiones por segundo
def rate_limit():
    current_time = time.time()  # Obtener la marca de tiempo actual
    request_times.append(current_time)  # Agregar la marca de tiempo actual a la cola
    if len(request_times) == 100:
        elapsed_time = current_time - request_times[0]  # Calcular el tiempo transcurrido desde la solicitud más antigua
        if elapsed_time < 120:
            wait_time = 120 - elapsed_time  # Calcular el tiempo de espera necesario
            print(f"Límite de peticiones alcanzado. Esperando {wait_time:.2f} segundos...")
            time.sleep(wait_time)  # Esperar el tiempo necesario

# Función para obtener el puuid de un jugador a partir de su nombre de invocador
def obtener_puuid(nombre_invocador):
    rate_limit()  # Verificar y gestionar el límite de peticiones
    url = f"{BASE_URL}riot/account/v1/accounts/by-riot-id/{nombre_invocador}?api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        info = response.json()
        return info["puuid"]
    else:
        print("Error al obtener el puuid del jugador:", response.status_code)
        print("Mensaje de error:", response.text)
        return None

# Función para obtener las partidas históricas de un jugador a partir de su puuid
def obtener_partidas_historicas(puuid):
    if not puuid:
        return []
    rate_limit()  # Verificar y gestionar el límite de peticiones
    url = f"{BASE_URL}lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={no_games}&api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener las partidas históricas del jugador:", response.status_code)
        print("Mensaje de error:", response.text)
        return []

# Función para analizar y almacenar los datos de una partida específica
def analizar_partida(match_id):
    match_data = get_match_data(match_id)
    if match_data:
        for puuid in match_data['metadata']['participants']:
            player_data = find_player_data(match_data, puuid)
            if player_data:
                data_all.append(player_data)
                # Enviar datos a Kafka
                producer.produce('riot-games-topic', key=puuid, value=json.dumps(player_data))
                producer.flush()  # Asegurar que los mensajes se envíen
        print(f'Partida {match_id} analizada')

# Función para obtener las partidas de cada jugador en una partida específica
def obtener_partidas_jugadores_en_partida(match_id):
    rate_limit()  # Verificar y gestionar el límite de peticiones
    url = f'{BASE_URL}lol/match/v5/matches/{match_id}?api_key={API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["info"]["queueId"] == 420:  # Verificar si la partida es del tipo que nos interesa
            for jugador in data["info"]["participants"]:
                puuid = jugador["puuid"]
                partidas = obtener_partidas_historicas(puuid)
                for p in partidas:
                    if p not in match_ids:
                        match_ids.append(p)
                        print(f'Hay {len(match_ids)} partidas registradas')
                        if len(match_ids) >= limite:
                            return match_ids
                        # Analizar la partida al momento
                        analizar_partida(p)
    else:
        print("Error al obtener los datos de la partida:", response.status_code)
        print("Mensaje de error:", response.text)
    return match_ids

# Función para obtener la información de una partida en base al match_id
def get_match_data(match_id):
    rate_limit()  # Verificar y gestionar el límite de peticiones
    url = f'{BASE_URL}lol/match/v5/matches/{match_id}?api_key={API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener los datos de la partida:", response.status_code)
        print("Mensaje de error:", response.text)
        return None

# Función para obtener los datos de un jugador en una partida específica
def find_player_data(match_data, puuid):
    if not match_data or 'metadata' not in match_data:
        return None
    participants = match_data['metadata']['participants']
    if puuid in participants:
        player_index = participants.index(puuid)
        if player_index < len(match_data['info']['participants']):
            return match_data['info']['participants'][player_index]
    return None

# Función principal para ejecutar el proceso de recolección de datos
def main():
    global match_ids, data_all
    match_ids = []
    data_all = []
    
    # Obtener el puuid del jugador de origen
    jugador_origen = obtener_puuid('Kresta/1302')
    # Obtener las partidas históricas del jugador de origen
    partidas_origen = obtener_partidas_historicas(jugador_origen)
    
    print(f'Comenzamos el bucle de extracción de los ID de las partidas hasta llegar a {limite}.')
    
    for partida in partidas_origen:
        # Obtener y analizar partidas de los jugadores en cada partida
        obtener_partidas_jugadores_en_partida(partida)
        if len(match_ids) >= limite:
            break

    print(f'Tenemos {len(match_ids)} ID de partidas únicas')
    
    # Guardar los IDs de las partidas y los datos de los jugadores en archivos JSON
    with open('match_ids.json', 'w') as f:
        json.dump(match_ids, f)
    
    with open('data_all.json', 'w') as f:
        json.dump(data_all, f)
    
    # Convertir los datos a un DataFrame de Pandas para su análisis posterior
    df = pd.DataFrame(data_all)
    print(df)

if __name__ == "__main__":
    main()

