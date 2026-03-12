#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

# Импортируем классы для работы с Kafka
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
# Импортируем классы для работы с Schema Registry и десериализации Avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class User(object):
    """
    Класс для представления записи пользователя (User).
    """

    # Инициализатор класса (исправлено с init на __init__)
    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
    """
    Функция-конвертер. Преобразует словарь (dict), полученный после 
    базовой десериализации Avro, в экземпляр класса User.
    """
    
    # Если данные пустые, возвращаем None
    if obj is None:
        return None

    # Создаем и возвращаем объект User, заполняя его полями из словаря
    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def main():
    print('Start Consumer')
    
    # Указываем топик, из которого будем читать сообщения
    topic = "<topic>"

    # Имя файла со схемой Avro
    schema = "user_specific.avsc"

    # Получаем абсолютный путь к директории, в которой находится данный скрипт
    # (исправлено с file на __file__)
    path = os.path.realpath(os.path.dirname(__file__))
    
    # Открываем и читаем файл схемы в строку
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    # Настройка клиента Schema Registry (адрес сервера со схемами)
    schema_conf = {'url': 'http://<ip>:8081/'}
    schema_registry_client = SchemaRegistryClient(schema_conf)

    # Создаем объект десериализатора Avro.
    # Передаем ему клиент Schema Registry, строку со схемой и функцию-конвертер
    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    # Оставлены только базовые и необходимые параметры настройки.
    consumer_conf = {
        'group.id': 'my-consumer-group',
        'bootstrap.servers': '<ip>:9092', # Адрес брокера Kafka
        'session.timeout.ms': 10000,                            # Таймаут сессии (10 секунд)
        'auto.offset.reset': 'earliest'                         # Если нет сохраненного оффсета, начинать чтение с самого начала
    }

    # Инициализируем консьюмер с нашей конфигурацией
    consumer = Consumer(consumer_conf)
    
    # Подписываемся на нужный топик (ожидается список топиков)
    consumer.subscribe([topic])

    # Запускаем бесконечный цикл для постоянного чтения сообщений из топика
    while True:
        try:
            # Пытаемся получить сообщение. Таймаут 1 секунда, чтобы можно было 
            # безопасно прервать выполнение (например, через Ctrl+C)
            msg = consumer.poll(1.0)
            
            # Если сообщений нет, просто переходим к следующей итерации цикла
            if msg is None:
                continue

            # Если сообщение получено, десериализуем его значение (value)
            # Передаем контекст: название топика и указываем, что десериализуем значение (VALUE), а не ключ
            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            # Если после десериализации получен объект user, выводим данные в консоль
            if user is not None:
                print("User record {}: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(msg.key(), user.name,
                              user.favorite_number,
                              user.favorite_color))
                              
        # Обработка прерывания с клавиатуры (Ctrl+C) для выхода из цикла
        except KeyboardInterrupt:
            break

    # Корректно закрываем консьюмер после выхода из цикла (освобождаем ресурсы, фиксируем оффсеты)
    consumer.close()

# Точка входа в программу (исправлено с name == 'main' на __name__ == '__main__')
if __name__ == '__main__':
    main()