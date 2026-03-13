package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class TestTemperatureProducer {
    public static void main(String[] args) throws InterruptedException {
        // Настройки продюсера
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        System.out.println("Начало отправки данных о температуре...");

        try {
            for (int i = 0; i < 100; i++) {
                // Генерируем случайную температуру от 10 до 30
                int temp = 10 + random.nextInt(21);
                String tempString = String.valueOf(temp);

                // Отправляем сообщение. Ключ не важен (алгоритм сам задаст ключ "temp"), отправляем null.
                ProducerRecord<String, String> record = new ProducerRecord<>("iot-temperature", null, tempString);
                producer.send(record);

                System.out.println("Отправлено: " + tempString + "°C");

                // Пауза 1 секунда (позволяет наглядно увидеть, как Streams разбивает данные на 5-секундные окна)
                Thread.sleep(1000);
            }
        } finally {
            producer.close();
            System.out.println("Продюсер остановлен.");
        }
    }
}
