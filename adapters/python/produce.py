import os
from uuid import uuid4
from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Схема Avro для нашего пользователя. 
# Она описывает структуру данных, которая будет отправляться в Kafka.
schema_str = """
{
    "namespace": "confluent.io.examples.serialization.avro",
    "name": "User",
    "type": "record",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": "int"},
        {"name": "favorite_color", "type": "string"}
    ]
}
"""

class User(object):
    """
    Класс для хранения данных пользователя.

    Аргументы:
        name (str): Имя пользователя
        favorite_number (int): Любимое число пользователя
        favorite_color (str): Любимый цвет пользователя
        address(str): Адрес пользователя (конфиденциальная информация, не отправляется)
    """

    # Исправлено с init на __init__ для правильной инициализации объекта в Python
    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # Поле _address не должно сериализоваться (превращаться в байты для отправки)
        self._address = address


def user_to_dict(user, ctx):
    """
    Преобразует объект класса User в словарь (dict) для последующей Avro сериализации.

    Аргументы:
        user (User): Экземпляр класса User.
        ctx (SerializationContext): Метаданные, относящиеся к операции сериализации.

    Возвращает:
        dict: Словарь с атрибутами пользователя, готовыми к сериализации.
    """
    # Поле user._address конфиденциально и не включается в словарь для отправки
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    """
    Функция обратного вызова (callback). Вызывается при успешной или неудачной доставке сообщения в Kafka.

    Аргументы:
        err (KafkaError): Ошибка, если она произошла, или None в случае успеха.
        msg (Message): Объект сообщения, которое было отправлено или не отправлено.
    """
    if err is not None:
        # Если есть ошибка, выводим сообщение об ошибке
        print("Ошибка доставки для записи пользователя {}: {}".format(msg.key(), err))
        return
    # Если ошибки нет, подтверждаем успешную отправку с указанием топика, партиции и смещения (offset)
    print('Запись пользователя {} успешно отправлена в топик {} [партиция {}] на смещение {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    print('Start Producer')
    topic = "<topic>"

    schema = "user_specific.avsc"

    # Исправлено file на __file__
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    schema_conf = {'url':'http://<ip>:8081/'}
    schema_registry_client = SchemaRegistryClient(schema_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    # Оставлен только адрес брокера, настройки SSL и Kerberos удалены
    producer_conf = {'bootstrap.servers': "<ip>:9092"}

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            user_name = input("Enter name: ")
            user_address = input("Enter address: ")
            user_favorite_number = int(input("Enter favorite number: "))
            user_favorite_color = input("Enter favorite color: ")
            user = User(name=user_name,
                        address=user_address,
                        favorite_color=user_favorite_color,
                        favorite_number=user_favorite_number)
            producer.produce(topic=topic,
                             value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


# Исправлено на корректную конструкцию __name__ == '__main__'
if __name__ == '__main__':
    main()