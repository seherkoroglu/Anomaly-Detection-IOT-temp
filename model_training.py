import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from kafka import KafkaConsumer, KafkaProducer
import json

# Kafka Ayarları
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC_INPUT = "iot_topic"
KAFKA_TOPIC_NORMAL = "iot_normal"
KAFKA_TOPIC_ANOMALY = "iot_anormal"

# Etiketli veriyi yükleme
data = pd.read_csv("labeled_data.csv")


# Logistic Regression Modeli
class RealTimeModel:
    def __init__(self):
        self.model = LogisticRegression(random_state=42, class_weight='balanced')
        self.scaler = StandardScaler()
        self.is_model_trained = False

    def train_model(self, data):
        """Modeli eğitmek için veriyi kullanır."""
        # Özellikler ve etiketleri ayırma
        X = data[['temp', 'out/in', 'category']]  # 'temp', 'out/in', 'category' özellikleri
        y = data['label']  # 'label' hedef etiketi

        try:
            # 'out/in' sütununu sayısal hale getirme
            X['out/in'] = X['out/in'].map({'In': 0, 'Out': 1})

        except Exception as e:
            print(f"Veri işlenemedi: {data} - Hata: {str(e)}")
            # Hata loglama ve veri atlama...

        # Özellikleri ölçeklendirme
        X_scaled = self.scaler.fit_transform(X)

        # Modeli eğitme
        self.model.fit(X_scaled, y)
        self.is_model_trained = True
        print("Model başarıyla eğitildi.")

    def predict(self, row_df):
        """Gerçek zamanlı tahmin yapma."""
        if not self.is_model_trained:
            raise ValueError("Model henüz eğitilmedi.")

        # 'out/in' sütununu sayısal hale getirme
        row_df['out/in'] = row_df['out/in'].map({'In': 0, 'Out': 1})

        # Özellikleri ölçeklendirme
        X_scaled = self.scaler.transform(row_df[['temp', 'out/in', 'category']])
        prediction = self.model.predict(X_scaled)
        return prediction


# Özellikler ve etiketleri ayırma
X = data[['temp', 'out/in', 'category']]  # 'temp', 'out/in', 'category' özellikleri
y = data['label']  # 'label' hedef etiketi

# 'out/in' sütunundaki değerleri 0 ve 1'e dönüştürme
X.loc[:, 'out/in'] = X['out/in'].map({'Out': 1, 'In': 0})

# Veriyi eğitim ve test setlerine ayırma
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Özellikleri ölçeklendirme (Z-skoru ile normalleştirme)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Ağırlıklı Loss Fonksiyonu ile Logistic Regression Modeli
model = LogisticRegression(random_state=42,
                           class_weight='balanced')  # class_weight='balanced' kullanarak sınıf ağırlıklarını dengeleme
model.fit(X_train_scaled, y_train)

# Modelin tahmin yapması
y_pred = model.predict(X_test_scaled)

# Model değerlendirme
accuracy = accuracy_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)
class_report = classification_report(y_test, y_pred)

# Sonuçları yazdırma
# Modelin eğitim seti üzerindeki tahminleri
y_train_pred = model.predict(X_train_scaled)

# Eğitim doğruluğu
train_accuracy = accuracy_score(y_train, y_train_pred)
print(f"Eğitim Doğruluğu: {train_accuracy * 100:.2f}%")
print("Logistic Regression Modelin Doğruluk Oranı: {:.2f}%".format(accuracy * 100))
print("Confusion Matrix:\n", conf_matrix)
print("Classification Report:\n", class_report)

import matplotlib.pyplot as plt

# Eğitim ve test doğruluğunun değerlerini saklayacağız
train_accuracies = []
test_accuracies = []



# Confusion Matrix görselleştirme
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", xticklabels=["Normal", "Anomalik"],
            yticklabels=["Normal", "Anomalik"])
plt.title("Logistic Regression Confusion Matrix")
plt.xlabel("Tahmin")
plt.ylabel("Gerçek")
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(range(1, 11), train_accuracies, label='Eğitim Doğruluğu', marker='o')
plt.plot(range(1, 11), test_accuracies, label='Test Doğruluğu', marker='o')
plt.title("Eğitim ve Test Doğruluğu")
plt.xlabel("Epoch")
plt.ylabel("Doğruluk")
plt.legend()
plt.grid(True)
plt.show()

# Kafka Tüketici ve Üretici
class KafkaConsumerProducer:
    def __init__(self, input_topic, normal_topic, anomaly_topic, server, model):
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=server,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.normal_producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.anomaly_producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.model = model

    def consume_process_produce(self):
        """Kafka'dan veri al, tahmin yap ve sonuçları yeni topic'lere gönder."""
        print("Kafka Consumer çalışıyor...")

        for message in self.consumer:
            data = message.value
            print(f"Kafka'dan gelen veri: {data}")

            # Gelen veriyi DataFrame'e dönüştür
            row_df = pd.DataFrame([data])

            # Modelin eğitilmiş olduğundan emin ol
            if self.model.is_model_trained:
                try:
                    prediction = self.model.predict(row_df)
                    data['prediction'] = 'Normal' if prediction[0] == 1 else 'Anomalik'

                    # Tahmin sonucuna göre veriyi ilgili topic'e gönder
                    if prediction[0] == 1:
                        self.normal_producer.send(KAFKA_TOPIC_NORMAL, value=data)
                        print(f"Normal veri gönderildi: {data}")
                    else:
                        self.anomaly_producer.send(KAFKA_TOPIC_ANOMALY, value=data)
                        print(f"Anomalik veri gönderildi: {data}")
                except ValueError as e:
                    print(f"Hata: {e} - Veri işlenemedi: {data}")
            else:
                print("Model henüz eğitilmedi. Veriyi işleyemiyorum.")


# Ana Çalışma Kısmı
if __name__ == "__main__":
    try:
        # Eğitim verisiyle modeli eğit
        training_data = pd.read_csv("labeled_data.csv")
        real_time_model = RealTimeModel()
        real_time_model.train_model(training_data)  # train_model fonksiyonu artık mevcut

        # Kafka tüketici-üretici başlat
        kafka_handler = KafkaConsumerProducer(
            KAFKA_TOPIC_INPUT,
            KAFKA_TOPIC_NORMAL,
            KAFKA_TOPIC_ANOMALY,
            KAFKA_SERVER,
            real_time_model
        )
        kafka_handler.consume_process_produce()
    except FileNotFoundError:
        print("Hata: Eğitim verisi dosyası bulunamadı.")
    except Exception as e:
        print(f"Beklenmeyen bir hata oluştu: {e}")
