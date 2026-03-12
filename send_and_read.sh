#!/bin/bash
set -xe

cd kafka_2.13-4.2.0

# Создаем топик, игнорируя ошибку, если он уже существует
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 --if-not-exists

# Отправляем 3 тестовых сообщения
echo -e "Test message 1\nTest message 2\nTest message 3" | bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092

# Вычитываем сообщения. 
# Обязательно указываем --max-messages 3, чтобы консьюмер завершился после прочтения 3 сообщений, а не висел бесконечно.
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092 --max-messages 3

set +xe
echo "Тест успешно завершен!"