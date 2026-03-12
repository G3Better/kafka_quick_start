# kafka_quick_start
download_and_install скрипт используется для скачивания kafka и локального запуска из одной ноды
send_and_read тестовый скрипт для отправки и скачивания нескольких сообщений в топик

## Python:
```
#Команды запуска
python ./consume.py
python ./produce.py

#Или Python3
python3 ./consume.py
python3 ./produce.py
```

## Java:
```
#сборка jar
mvn clean package
```

## Берём файл jar из target:
```
#Команды запуска:
java -cp KafkaTest-1.0-SNAPSHOT.jar org.example.ProducerExample
java -cp KafkaTest-1.0-SNAPSHOT.jar org.example.ConsumerExample
```

## Go:
```
#Команды запуска:
go run producer.go config.yaml
go run consumer.go config.yaml
```

## AKHQ
```
#команды запуска:
cd akhq-setup
docker-compose up -d
```
по умолчанию будет доступна на порту 8080