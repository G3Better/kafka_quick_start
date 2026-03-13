package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TestConsumerForWordCountDemo {

    public static final String OUTPUT_TOPIC = "quickstart-events-output";

    public static void main(String[] args) {
        Properties props = new Properties();

        // Адрес Kafka брокера (как в вашем Streams приложении)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");

        // ID группы консьюмеров
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-result-reader-group");

        // Читаем с самого начала, если нет сохраненного оффсета
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ВАЖНО: Ключ десериализуем как String, а значение как Long
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        // Создаем консьюмера
        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props)) {
            // Подписываемся на топик с результатами
            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

            System.out.println("Ожидание данных из топика " + OUTPUT_TOPIC + "...");

            // Бесконечный цикл чтения
            while (true) {
                ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Long> record : records) {
                    System.out.printf("Слово: '%s', Количество: %d (offset = %d)%n",
                            record.key(),
                            record.value(),
                            record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
