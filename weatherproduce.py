import requests
from kafka import KafkaProducer
import json
import time

# Kafka configuration
KAFKA_TOPIC = "weather_data"
KAFKA_BROKER = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# WeatherAPI configuration
API_KEY = "99392a31bc6a4044abf64812241206"
CITY_NAME = "Kochi"
WEATHER_API_URL = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY_NAME}"

def fetch_weather_data():
    response = requests.get(WEATHER_API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def produce_weather_data():
    while True:
        weather_data = fetch_weather_data()
        if weather_data:
            producer.send(KAFKA_TOPIC, weather_data)
            print(f"Produced: {weather_data}")
        time.sleep(60)  # Fetch data every minute

if __name__ == "__main__":
    produce_weather_data()