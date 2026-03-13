import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestTemperatureConsumer {
    public static void main(String[] args) {
        // Настройки консьюмера
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<ip>:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "temperature-max-reader");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Создаем десериализатор для оконного ключа.
        // 5000L - это размер окна в миллисекундах (5 секунд), как в Streams-приложении.
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), 5000L);
        StringDeserializer stringDeserializer = new StringDeserializer();

        // Передаем десериализаторы явно через конструктор
        KafkaConsumer<Windowed<String>, String> consumer = new KafkaConsumer<>(props, windowedDeserializer, stringDeserializer);

        // Подписываемся на выходной топик
        consumer.subscribe(Collections.singletonList("iot-temperature-max"));

        System.out.println("Ожидание данных с максимальной температурой (>20°C)...");

        // Форматтер для красивого вывода времени окна
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

        try {
            while (true) {
                ConsumerRecords<Windowed<String>, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<Windowed<String>, String> record : records) {
                    Windowed<String> windowedKey = record.key();
                    String maxTemp = record.value();

                    // Извлекаем время начала и конца окна
                    Instant start = windowedKey.window().startTime();
                    Instant end = windowedKey.window().endTime();

                    System.out.printf("Окно [%s - %s] | Макс. температура: %s°C%n",
                            formatter.format(start),
                            formatter.format(end),
                            maxTemp
                    );
                }
            }
        } finally {
            consumer.close();
        }
    }
}
