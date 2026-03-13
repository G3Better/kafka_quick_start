package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TestProducerForWordCountDemo {

    public static void main(String[] args) {
        // 1. Настройка конфигурации продюсера
        Properties props = new Properties();
        // Указываем адрес брокера (тот же, что и в вашем Streams приложении)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");
        // Сериализаторы для ключа и значения (оба типа String)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Создание экземпляра продюсера
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Имя входного топика, из которого читает ваше Streams приложение
        String topic = "quickstart-events";

        // Тестовые данные (строки текста)
        String[] testMessages = {
                "Hello Kafka",
                "Hello World",
                "Kafka Streams is awesome",
                "Hello Streams"
        };

        // 3. Отправка сообщений
        try {
            for (String text : testMessages) {
                // Создаем запись. Ключ не указываем (null), передаем только значение (текст)
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, text);

                // Отправляем сообщение
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Отправлено сообщение: '" + text +
                                "' в партицию " + metadata.partition() +
                                " с оффсетом " + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });

                // Небольшая задержка, чтобы имитировать поток данных
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 4. Обязательно закрываем продюсер для освобождения ресурсов
            producer.flush();
            producer.close();
            System.out.println("Продюсер завершил работу.");
        }
    }
}
