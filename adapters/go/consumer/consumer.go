package main

// consumer_example реализует Kafka-консьюмер, использующий неканальный API Poll()
// для получения сообщений и событий. Консьюмер также использует Schema Registry для десериализации Avro.

import (
 "fmt"
 "os"
 "os/signal"
 "syscall"

 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"

 "github.com/spf13/viper"
)

func main() {
 // 1. Проверка аргументов командной строки.
 // Ожидается, что путь к конфигурационному файлу будет передан первым аргументом.
 if len(os.Args) < 2 {
  fmt.Fprintf(os.Stderr, "Usage: %s /config.yaml\n", os.Args[0])
  os.Exit(1)
 }

 // Карта для хранения настроек Kafka
 kafkaProps := kafka.ConfigMap{}
 fileName := os.Args[1]

 // 2. Настройка Viper для чтения конфигурации из YAML файла
 viper.SetConfigType("yaml")
 viper.SetConfigName(fileName)
 viper.AddConfigPath(".")
 
 // Чтение файла конфигурации
 err := viper.ReadInConfig()
 if err != nil {
  panic(err) // Останавливаем выполнение, если файл не найден или поврежден
 }
 
 // Получаем все ключи из прочитанного конфига
 keys := viper.AllKeys()

 // 3. Заполнение kafkaProps на основе файла конфигурации
 for _, key := range keys {
  var v string
  v = fmt.Sprint(viper.Get(key))
  fmt.Println(key, ":", v)
  
  // Исключаем настройки, которые не относятся напрямую к клиенту Kafka,
  // так как schema.registry.url и topic нужны нам для других целей.
  if key != "schema.registry.url" && key != "topic" {
   kafkaProps[key] = v
  }
 }

 // 4. Извлечение специфичных параметров
 // URL для подключения к Schema Registry
 url := fmt.Sprint(viper.Get("schema.registry.url"))
 // Название топика, на который будем подписываться (оборачиваем в срез строк)
 topics := []string{fmt.Sprint(viper.Get("topic"))}

 // 5. Настройка обработки системных сигналов для gracefully shutdown (мягкого завершения)
 // Создаем канал, который будет получать сигналы прерывания (Ctrl+C или SIGTERM)
 sigchan := make(chan os.Signal, 1)
 signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

 // 6. Создание Kafka Consumer с подготовленными настройками
 c, err := kafka.NewConsumer(&kafkaProps)
 if err != nil {
  fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
  os.Exit(1)
 }
 fmt.Printf("Created Consumer %v\n", c)

 // 7. Создание клиента Schema Registry
 client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
 if err != nil {
  fmt.Printf("Failed to create schema registry client: %s\n", err)
  os.Exit(1)
 }

 // 8. Создание Avro десериализатора
 // Он будет преобразовывать бинарные данные из Kafka в Go-структуры с помощью схем из Registry
 deser, err := avro.NewGenericDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
 if err != nil {
  fmt.Printf("Failed to create deserializer: %s\n", err)
  os.Exit(1)
 }

 // 9. Подписка на указанные топики
 err = c.SubscribeTopics(topics, nil)
 if err != nil {
  fmt.Printf("Failed to subscribe to topics: %s\n", err)
  os.Exit(1)
 }

 run := true

 // 10. Основной цикл опроса (Polling)
 for run {
  select {
  case sig := <-sigchan:
   // Если пришел системный сигнал (например, завершение программы)
   fmt.Printf("Caught signal %v: terminating\n", sig)
   run = false // Прерываем цикл
  default:
   // Опрос Kafka каждые 100 миллисекунд
   ev := c.Poll(100)
   if ev == nil {
    continue // Нет событий, продолжаем цикл
   }

   // Проверка типа полученного события
   switch e := ev.(type) {
   case *kafka.Message:
    // Если это сообщение, десериализуем его
    value := User{}
    
    // Десериализация Avro Payload прямо в структуру User
    err := deser.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
    if err != nil {
     fmt.Printf("Failed to deserialize payload: %s\n", err)
    } else {
     fmt.Printf("%% Message on %s:\n%+v\n", e.TopicPartition, value)
    }
    
    // Вывод заголовков сообщения, если они есть
    if e.Headers != nil {
     fmt.Printf("%% Headers: %v\n", e.Headers)
    }
   case kafka.Error:
    // Ошибки Kafka обычно носят информационный характер;
    // клиент попытается восстановиться автоматически.
    fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
   default:
    // Прочие события (например, изменение партиций)
    fmt.Printf("Ignored %v\n", e)
   }
  }
 }

 // 11. Мягкое закрытие соединений
 fmt.Printf("Closing consumer\n")
 c.Close()
}


type User struct {
 Name           `string json:"name"`
 FavoriteNumber `int64  json:"favorite_number"`
 FavoriteColor  `string json:"favorite_color"`
}