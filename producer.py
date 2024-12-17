import pandas as pd
from kafka import KafkaProducer
import json
import time
from sklearn.preprocessing import StandardScaler

class DataLoader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def load_data(self):
        """Veriyi CSV dosyasından yükler."""
        self.data = pd.read_csv(self.file_path)
        print("Veri yüklendi.")
        print(self.data.head())  # İlk 5 satırı gösterelim
        return self.data


class MissingValueHandler:
    def __init__(self, data):
        self.data = data

    def fill_missing_values(self):
        """Eksik değerleri doldurur."""
        self.data['temp'].fillna(self.data['temp'].mean(), inplace=True)  # Yalnızca 'temp' sütununu ele alıyoruz
        print("Eksik değerler dolduruldu.")
        print(self.data.head())  # Doldurulmuş veriyi görelim
        return self.data


class DataNormalizer:
    def __init__(self, data):
        self.data = data
        self.scaler = StandardScaler()

    def normalize_data(self):
        """Veriyi Z-skoru ile normalleştirir."""
        # 'temp' sütununu normalleştiriyoruz
        self.data['temp'] = self.scaler.fit_transform(self.data[['temp']])
        print("Veri normalleştirildi (Z-skoru ile).")
        print("Normalleştirilmiş 'temp' verisi:")
        print(self.data['temp'].head())  # Normalleştirilmiş sıcaklık verisi
        return self.data


class TemperatureCategorizer:
    def __init__(self, data):
        self.data = data

    def categorize_temperature_zscore(self, row):
        """Sıcaklık verisini kategorilere ayırır."""
        temp = row['temp']  # 'temp' sütununa erişim
        if temp < -2:
            return "Very Low"
        elif -2 <= temp < -1:
            return "Low"
        elif -1 <= temp < 0:
            return "Medium Low"
        elif 0 <= temp < 1:
            return "Medium"
        elif 1 <= temp < 2:
            return "Medium High"
        else:
            return "High"

    def label_anomalies(self):
        """Anomali etiketlemesi yapar."""
        # Anomalik veriyi etiketleme: Sıcaklık verilerini daha fazla kategorize ederek etiketleri artırıyoruz.
        self.data['label'] = self.data['temp'].apply(
            lambda temp: 0 if temp < 0 or temp > 2 else 1  # 0: Anomalik (Very Low ve Very High), 1: Normal (diğerleri)
        )

        print("Veri etiketlendi:")
        print(self.data[['temp', 'label']].head())  # Etiketlenmiş veri
        return self.data

    def process_data(self):
        """Sıcaklık verisini normalize et ve kategorilere ayır."""
        # Sıcaklık kategorilerini belirle
        self.data['category'] = self.data.apply(lambda row: self.categorize_temperature_zscore(row), axis=1)
        print("Kategoriler belirlendi: ")
        print(self.data['category'].unique())  # 'category' sütununun tüm farklı değerlerini göster

        # Kategorileri sayısal değerlere dönüştür
        category_mapping = {
            'Very Low': 0,
            'Low': 1,
            'Medium Low': 2,
            'Medium': 3,
            'Medium High': 4,
            'High': 5
        }
        self.data['category'] = self.data['category'].map(category_mapping)

        print("Sayısal kategoriler: ")
        print(self.data['category'].unique())  # Sayısal kategoriler
        return self.data


class KafkaSender:
    def __init__(self, data, kafka_topic, kafka_server):
        self.data = data
        self.kafka_topic = kafka_topic
        self.kafka_server = kafka_server
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def send_data_to_kafka(self):
        """Veriyi Kafka'ya gönder."""
        for _, row in self.data.iterrows():
            message = row.to_dict()
            self.producer.send(self.kafka_topic, value=message)
            print(f"Veri Kafka'ya gönderildi: {message}")
            time.sleep(1)  # Veriyi göndermeden önce 1 saniye bekleme


# Veri pipeline işlemleri
data_loader = DataLoader(file_path="IOT-temp.csv")
data = data_loader.load_data()

missing_value_handler = MissingValueHandler(data)
data = missing_value_handler.fill_missing_values()

data_normalizer = DataNormalizer(data)
data = data_normalizer.normalize_data()

temperature_categorizer = TemperatureCategorizer(data)
data = temperature_categorizer.label_anomalies()  # Etiketleme işlemi burada yapılmalı
data = temperature_categorizer.process_data()

# Etiketli veriyi bir CSV dosyasına kaydet
data.to_csv("labeled_data.csv", index=False)
print("Veri dosyaya kaydedildi: labeled_data.csv")

kafka_sender = KafkaSender(data, kafka_topic="iot_topic", kafka_server="localhost:9092")
kafka_sender.send_data_to_kafka()
