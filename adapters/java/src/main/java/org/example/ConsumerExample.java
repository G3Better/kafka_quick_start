package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {
    public static void main(String[] args) {
        String schemaRegistryUrl = "http://<ip>:8081";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-user-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Используем наш кастомный десериализатор
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomAvroDeserializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        String topic = "<topic>"; // Укажите ваш топик

        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Ожидание сообщений со схемой User...");

            int emptyPollCount = 0;
            int maxEmptyPolls = 30; // Количество пустых пулов до выхода (5 пулов по 1 сек = 5 секунд)

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    if (emptyPollCount >= maxEmptyPolls) {
                        System.out.println("Нет новых сообщений в течение " + maxEmptyPolls + " секунд. Завершение работы консьюмера...");
                        break; // Выходим из цикла
                    }
                } else {
                    // Сбрасываем счетчик, так как получили сообщения
                    emptyPollCount = 0;

                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        GenericRecord user = record.value();

                        if (user != null) {
                            // Извлекаем поля в соответствии с вашей схемой
                            String name = user.get("name").toString();
                            long favoriteNumber = (Long) user.get("favorite_number");
                            // favorite_color может быть null в некоторых схемах, проверяем это
                            Object colorObj = user.get("favorite_color");
                            String favoriteColor = colorObj != null ? colorObj.toString() : "null";

                            System.out.printf("Получен User: Имя = %s, Любимое число = %d, Любимый цвет = %s (offset=%d)%n",
                                    name, favoriteNumber, favoriteColor, record.offset());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close(); // Корректно закрываем консьюмер после выхода из цикла
            System.out.println("Консьюмер успешно закрыт.");
        }
    }
}
