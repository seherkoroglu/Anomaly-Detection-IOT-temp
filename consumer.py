from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import json
from kafka import KafkaConsumer, KafkaProducer
import time

# Spark Session başlatma
spark = SparkSession.builder \
    .appName("RealTimeAnomalyDetection") \
    .getOrCreate()

# Kafka Consumer ile Kafka'dan veri al
consumer = KafkaConsumer(
    'iot_topic',
    bootstrap_servers=['localhost:9092'],
    group_id='iot_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer ile anomali verilerini Kafka'ya gönder
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Anomali tespiti fonksiyonu
def is_anomalous(temp, mean, std_dev, threshold=3):
    """Sıcaklık değeri ile anomali tespiti yapar"""
    if temp is None:
        return False
    return abs((temp - mean) / std_dev) > threshold

# Ortalama ve standart sapmayı hesaplama
mean_temp = 25  # Sabit ortalama sıcaklık değeri
std_dev_temp = 5  # Sabit standart sapma değeri

# UDF fonksiyonunu tanımla
anomaly_udf = udf(lambda temp: is_anomalous(temp, mean_temp, std_dev_temp), BooleanType())

# Spark DataFrame'de anormal verileri saklamak için boş bir liste
anomalous_data = []

# Kafka'dan veri al ve anomali tespiti yap
for message in consumer:
    data = message.value

    # Veriyi JSON'dan çıkart
    timestamp = data['timestamp']
    room_id = data['room_id']
    temp = data['temp']
    category = data['category']

    # Anomali tespiti
    anomaly = is_anomalous(temp, mean_temp, std_dev_temp)

    if anomaly:  # Yalnızca anomali varsa veriyi Kafka'ya gönder
        result = {
            'timestamp': timestamp,
            'room_id': room_id,
            'temp': temp,
            'category': category,
            'anomaly': anomaly
        }

        # Anomali tespit sonuçlarını Kafka'ya gönder
        producer.send('iot_anomaly_topic', value=result)
        print(f"Anomali sonucu gönderildi: {result}")

        # Anomalileri listeye ekle
        anomalous_data.append((timestamp, room_id, temp, category, anomaly))

    # Veriyi 1 saniye bekleyerek gönderme
    time.sleep(1)

# Anomalileri bir DataFrame'e dönüştür
anomalous_df = spark.createDataFrame(anomalous_data, ["timestamp", "room_id", "temp", "category", "anomaly"])

# Anomalileri göster
anomalous_df.show()

consumer.close()

