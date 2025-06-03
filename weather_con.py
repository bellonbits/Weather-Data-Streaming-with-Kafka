from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather-nairobi-uk',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for weather data...\n")
for message in consumer:
    weather = message.value
    print(f"{weather['city']}, {weather['country']} - {weather['description'].capitalize()} at {weather['temperature']}°C")
