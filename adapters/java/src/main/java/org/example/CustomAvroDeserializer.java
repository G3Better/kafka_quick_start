package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomAvroDeserializer implements Deserializer<GenericRecord> {
    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Задаем ту же самую схему, что и в продюсере
        String userSchemaString = "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"User\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"name\", \"type\": \"string\"},\n"
                + "    {\"name\": \"favorite_number\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
                + "    {\"name\": \"favorite_color\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
                + "  ]\n"
                + "}";
        this.schema = new Schema.Parser().parse(userSchemaString);
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            // Читаем чистый Avro без дополнительных заголовков
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Avro message", e);
        }
    }

    @Override
    public void close() {
        // Ничего не нужно закрывать
    }
}
