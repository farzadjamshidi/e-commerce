package com.ecommerce.inventory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

// Turns a reply into the bytes a Kafka Avro consumer expects: a magic byte, the schema id, then the Avro data.
// It runs here and not in Debezium, so the schema is the one written in avro/, not one guessed from the data.
@Component
class AvroEncoder {

    private final KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    private final Schema stockReserved = load("avro/StockReserved.avsc");

    AvroEncoder(@Value("${schema-registry.url}") String schemaRegistryUrl) {
        serializer.configure(Map.of(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                // inventory carries several record types, and the default allows one schema per topic
                KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName()),
                false);
    }

    byte[] encode(String topic, StockReservedEvent event) {
        GenericRecord record = new GenericData.Record(stockReserved);
        record.put("eventId", event.eventId().toString());
        record.put("occurredAt", event.occurredAt().toEpochMilli());
        record.put("orderId", event.orderId().toString());
        return serializer.serialize(topic, record);
    }

    private static Schema load(String path) {
        try (InputStream in = AvroEncoder.class.getClassLoader().getResourceAsStream(path)) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
