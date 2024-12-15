Bu proje, **sınıf dengesizliği** problemini çözmek için **SMOTE** yöntemi kullanarak lojistik regresyon modeli ile sınıflandırma yapmaktadır. Modelin doğruluğunu artırmak için ağırlıklı sınıf fonksiyonu kullanılır. Ayrıca **öğrenme eğrisi** ile modelin performansı görselleştirilir.

---

## Gereksinimler

Proje için aşağıdaki kütüphaneler gereklidir:

- `pandas`
- `scikit-learn`
- `imblearn`
- `matplotlib`
- `seaborn`
- `kafka-python`

Kurulum için aşağıdaki komutu kullanabilirsiniz:

- pip install pandas scikit-learn imbalanced-learn matplotlib seaborn kafka-python

## Adımlar
1. Kafka'yı Başlatın
Önce Zookeeper'ı, ardından Kafka sunucusunu başlatmalısınız.

Zookeeper'ı Başlatma:
- zookeeper-server-start.sh config/zookeeper.properties
Kafka'yı Başlatma:
- kafka-server-start.sh config/server.properties

 Veri Setini Yükleyin
- Projede, labeled_data.csv adlı veri setini kullanılmaktadır. Bu dosyayı proje dizinine ekleyin.


Modeli eğitmek için aşağıdaki komutu çalıştırın:
- python model_training.py


Model eğitildikten sonra aşağıdaki çıktıları elde edeceksiniz:

- Doğruluk Oranı
- Confusion Matrix
- Classification Report
- Öğrenme Eğrisi Grafiği

Sonuçlar terminale yazdırılacak ve grafikler görselleştirilecektir.

## Çıktılar
1. Doğruluk Oranı
- Modelin doğruluk oranı yüzde cinsinden yazdırılır.

2. Confusion Matrix
- Aşağıdaki gibi bir görselleştirme elde edilecektir:

[[TP, FP],
 [FN, TN]]

3. Classification Report
- Precision, Recall, F1-Score gibi metrikler detaylı şekilde görüntülenir.

- Öğrenme Eğrisi Grafiği
Modelin eğitim ve test doğrulukları karşılaştırmalı olarak görselleştirilir.

Not: 
Kafka sunucusunu ve Zookeeper'ı kapatmayı unutmayın:

- kafka-server-stop.sh
- zookeeper-server-stop.sh

## License

[MIT](https://choosealicense.com/licenses/mit/)
