package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerExample {
    public static void main(String[] args) {

        // 1. Настройки Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Используем стандартный сериализатор массивов байтов вместо Confluent
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // 2. Создание экземпляра KafkaProducer (ключ - String, значение - byte[])
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        String topic = "<topic>";

        // Определяем схему User
        String userSchemaString = "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"User\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"name\", \"type\": \"string\"},\n"
                + "    {\"name\": \"favorite_number\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
                + "    {\"name\": \"favorite_color\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
                + "  ]\n"
                + "}";
        Schema schema = new Schema.Parser().parse(userSchemaString);

        try {
            // 3. Проверка доступности топика
            System.out.println("Проверка доступности топика '" + topic + "'...");
            if (producer.partitionsFor(topic) == null || producer.partitionsFor(topic).isEmpty()) {
                System.err.println("Ошибка: Топик '" + topic + "' не найден на брокере!");
                return; // Прерываем выполнение, если топика нет
            }
            System.out.println("Топик успешно найден. Начинаем отправку сообщений...");

            // Создаем writer один раз перед циклом для оптимизации
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            // 4. Отправка сообщений в цикле
            for (int i = 0; i < 5; i++) {
                String key = "Key-" + i;

                // Создаем Avro-запись по схеме
                GenericRecord user = new GenericData.Record(schema);
                user.put("name", "User_" + i);
                user.put("favorite_number", (long) (i * 10));
                user.put("favorite_color", (i % 2 == 0) ? "Blue" : "Red"); // чередуем цвета

                // Ручная сериализация Avro в byte[]
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

                writer.write(user, encoder);
                encoder.flush();
                out.close();

                byte[] serializedUser = out.toByteArray();

                // Отправка сериализованных байтов в Kafka
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, serializedUser);
                producer.send(record); // асинхронная отправка
                System.out.println("Отправлено сообщение: " + user.toString());
            }
        } catch (IOException e) {
            System.err.println("Ошибка сериализации Avro: " + e.getMessage());
        } finally {
            producer.close();
        }
    }
}
