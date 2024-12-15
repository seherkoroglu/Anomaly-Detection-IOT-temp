import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import learning_curve

# Etiketli veriyi yükleme
data = pd.read_csv("labeled_data.csv")

# Özellikler ve etiketleri ayırma
X = data[['temp','out/in', 'category']]  # 'temp' ve 'category' özellikleri
y = data['label']  # 'label' hedef etiketi

# 'out/in' sütunundaki değerleri 0 ve 1'e dönüştürme
X.loc[:, 'out/in'] = X['out/in'].map({'Out': 1, 'In': 0})

# Veriyi eğitim ve test setlerine ayırma
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Özellikleri ölçeklendirme (Z-skoru ile normalleştirme)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# SMOTE uygulaması (Azınlık sınıfını çoğaltma)
smote = SMOTE(random_state=42)
X_train_smote, y_train_smote = smote.fit_resample(X_train_scaled, y_train)

# SMOTE sonrası eğitim seti boyutları
print("SMOTE sonrası eğitim seti boyutu:", X_train_smote.shape)

# Ağırlıklı Loss Fonksiyonu ile Logistic Regression Modeli
model = LogisticRegression(random_state=42, class_weight='balanced')  # class_weight='balanced' kullanarak sınıf ağırlıklarını dengeleme
model.fit(X_train_smote, y_train_smote)

# Modelin tahmin yapması
y_pred_smote = model.predict(X_test_scaled)

# Model değerlendirme
accuracy_smote = accuracy_score(y_test, y_pred_smote)
conf_matrix_smote = confusion_matrix(y_test, y_pred_smote)
class_report_smote = classification_report(y_test, y_pred_smote)

# Sonuçları yazdırma
print("SMOTE ile Ağırlıklı Modelin Doğruluk Oranı: {:.2f}%".format(accuracy_smote * 100))
print("SMOTE ile Ağırlıklı Confusion Matrix:\n", conf_matrix_smote)
print("SMOTE ile Ağırlıklı Classification Report:\n", class_report_smote)

# Confusion Matrix görselleştirme
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix_smote, annot=True, fmt="d", cmap="Blues", xticklabels=["Normal", "Anomalik"], yticklabels=["Normal", "Anomalik"])
plt.title("SMOTE ile Ağırlıklı Confusion Matrix")
plt.xlabel("Tahmin")
plt.ylabel("Gerçek")
plt.show()

# Öğrenme eğrisini çizme (Train ve Test doğruluğu)
train_sizes, train_scores, test_scores = learning_curve(model, X_train_smote, y_train_smote, cv=5, n_jobs=-1,
                                                         train_sizes=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])

# Ortalama doğruluk skorlarını hesaplama
train_mean = train_scores.mean(axis=1)
test_mean = test_scores.mean(axis=1)

# Grafiği çizme
plt.figure(figsize=(10, 6))
plt.plot(train_sizes, train_mean, label='Train Accuracy', color='blue', marker='o')
plt.plot(train_sizes, test_mean, label='Test Accuracy', color='red', marker='s')
plt.title('Learning Curve: Train vs Test Accuracy')
plt.xlabel('Training Set Size')
plt.ylabel('Accuracy')
plt.legend()
plt.grid(True)
plt.show()
