from kafka import KafkaConsumer
import json

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC_NORMAL = "iot_normal"

print("Normal verileri gösteren consumer çalışıyor...")
consumer = KafkaConsumer(
    KAFKA_TOPIC_NORMAL,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(f"Normal Veri: {message.value}")

