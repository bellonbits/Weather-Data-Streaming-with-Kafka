import requests
import json
import time
from kafka import KafkaProducer

API_KEY = "c32170926bb267e9e09b323867c11c47"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = [("Nairobi", "KE"), ("London", "GB")]

def fetch_weather(city, country):
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return {
            "city": city,
            "country": country,
            "timestamp": int(time.time()),
            "temperature": response.json()['main']['temp'],
            "description": response.json()['weather'][0]['description']
        }
    else:
        print(f"Error fetching weather for {city}: {response.json().get('message')}")
        return None

while True:
    for city, country in cities:
        weather = fetch_weather(city, country)
        if weather:
            producer.send("weather-nairobi-uk", value=weather)
            print("Produced:", weather)
    time.sleep(30)
