#!/bin/bash

# Проверяем, передан ли аргумент с IP-адресом
if [ -z "$1" ]; then
  echo "Ошибка: Не указан IP-адрес."
  echo "Использование: $0 <IP-адрес>"
  exit 1
fi

ADVERTISED_IP="$1"

set -xe

# Используем curl -O для скачивания файла
curl -O https://dlcdn.apache.org/kafka/4.2.0/kafka_2.13-4.2.0.tgz

# Распаковка
tar -xzf kafka_2.13-4.2.0.tgz
cd kafka_2.13-4.2.0

# Генерация UUID кластера
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Форматирование хранилища
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

# Настройка listeners
sed -i 's|listeners=PLAINTEXT://:9092,CONTROLLER://:9093|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://:9093|g' config/server.properties
# Используем двойные кавычки для подстановки переменной
sed -i "s|advertised.listeners=PLAINTEXT://localhost:9092|advertised.listeners=PLAINTEXT://${ADVERTISED_IP}:9092|g" config/server.properties

# Запуск Kafka в фоновом режиме (флаг -daemon)
bin/kafka-server-start.sh -daemon config/server.properties

set +xe

echo "Kafka успешно запущена в фоновом режиме на IP: ${ADVERTISED_IP}"
