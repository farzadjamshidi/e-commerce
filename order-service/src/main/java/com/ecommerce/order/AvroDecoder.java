package com.ecommerce.order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

// Reads with the writer's schema, fetched from the registry by the id in the message. This service keeps no copy of
// inventory's schema and reads fields by name, so a field added later does not break it.
@Component
class AvroDecoder {

    private final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    AvroDecoder(@Value("${schema-registry.url}") String schemaRegistryUrl) {
        deserializer.configure(Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
    }

    GenericRecord decode(String topic, byte[] payload) {
        return (GenericRecord) deserializer.deserialize(topic, payload);
    }
}
