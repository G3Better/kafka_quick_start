package main

import (
 "fmt"
 "os"

 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
 "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"

 "github.com/spf13/viper"
)

func main() {
 // 1. Проверка аргументов командной строки.
 // Ожидаем путь к файлу конфигурации YAML.
 if len(os.Args) < 2 {
  fmt.Fprintf(os.Stderr, "Usage: %s /config.yaml\n", os.Args[0])
  os.Exit(1)
 }

 kafkaProps := kafka.ConfigMap{}
 // Получаем имя файла из аргументов
 fileName := os.Args[1]

 // 2. Настройка Viper для чтения конфигурации из YAML
 viper.SetConfigType("yaml")
 viper.SetConfigName(fileName)
 viper.AddConfigPath(".")

 // Чтение конфигурации
 err := viper.ReadInConfig()
 if err != nil {
  panic(err)
 }
 
 // Получаем все ключи из конфигурации
 keys := viper.AllKeys()

 // 3. Установка свойств Kafka из файла
 for _, key := range keys {
  var v string
  v = fmt.Sprint(viper.Get(key))
  fmt.Println(key, ":", v)
  
  // Отфильтровываем ключи, которые предназначены для Schema Registry и топика,
  // чтобы в настройки Kafka попали только ее собственные параметры
  if key != "schema.registry.url" && key != "topic" {
   kafkaProps[key] = v
  }
 }

 // 4. Получение URL Schema Registry и названия топика
 url := fmt.Sprint(viper.Get("schema.registry.url"))
 topic := fmt.Sprint(viper.Get("topic"))

 // 5. Создание Kafka Producer
 p, err := kafka.NewProducer(&kafkaProps)
 if err != nil {
  fmt.Printf("Failed to create producer: %s\n", err)
  os.Exit(1)
 }
 fmt.Printf("Created Producer %v\n", p)

 // 6. Создание клиента Schema Registry
 client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
 if err != nil {
  fmt.Printf("Failed to create schema registry client: %s\n", err)
  os.Exit(1)
 }

 // 7. Создание Avro сериализатора
 // Он будет преобразовывать структуру Go в бинарный формат Avro
 ser, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
 if err != nil {
  fmt.Printf("Failed to create serializer: %s\n", err)
  os.Exit(1)
 }

 // 8. Канал для получения отчета о доставке (Delivery Report)
 // Если его не указать, события будут отправляться в глобальный канал p.Events
 deliveryChan := make(chan kafka.Event)

 // 9. Подготовка данных для отправки
 value := User{
  Name:           "First user",
  FavoriteNumber: 42,
  FavoriteColor:  "blue",
 }
 
 // Сериализация структуры в массив байтов (Avro payload)
 payload, err := ser.Serialize(topic, &value)
 if err != nil {
  fmt.Printf("Failed to serialize payload: %s\n", err)
  os.Exit(1)
 }

 // 10. Отправка (Produce) сообщения в Kafka
 err = p.Produce(&kafka.Message{
  TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
  Value:          payload,
  Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
 }, deliveryChan)
 
 if err != nil {
  fmt.Printf("Produce failed: %v\n", err)
  os.Exit(1)
 }

 // 11. Ожидание ответа от брокера (успех или ошибка доставки)
 e := <-deliveryChan
 m := e.(*kafka.Message)

 // Проверка наличия ошибки в отчете о доставке
 if m.TopicPartition.Error != nil {
  fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
 } else {
  fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
   *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
 }

 // Закрываем канал
 close(deliveryChan)
}

type User struct {
 Name           string `json:"name"`
 FavoriteNumber int64  `json:"favorite_number"`
 FavoriteColor  string `json:"favorite_color"`
}
