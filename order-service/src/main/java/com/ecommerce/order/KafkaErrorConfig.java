package com.ecommerce.order;

import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
class KafkaErrorConfig {

    // Blocking retry, not retry topics: the partition waits, so the replies of an order stay in order.
    // After the last attempt the record goes to <topic>-dlt, in the same partition number.
    @Bean
    DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations,
                                     ProducerFactory<Object, Object> producerFactory) {
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2);
        backOff.setMaxElapsedTime(7000);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(templatesByValueType(kafkaOperations, producerFactory)), backOff);
        // A broken payload stays broken, so retrying only blocks the partition. AvroRuntimeException is a record that
        // does not match what this service reads, such as a missing field.
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class,
                AvroRuntimeException.class);
        // With manual acks nobody else commits the skipped record, and a restart would send it to the DLT again
        handler.setCommitRecovered(true);
        return handler;
    }

    // The inventory listener reads bytes and the payments listener reads strings, so a dead letter has to be
    // written back with the serializer that matches its value. If it cannot be written, it is retried forever.
    private static Map<Class<?>, KafkaOperations<?, ?>> templatesByValueType(
            KafkaOperations<Object, Object> stringTemplate, ProducerFactory<Object, Object> producerFactory) {
        KafkaTemplate<String, byte[]> bytesTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(), new StringSerializer(), new ByteArraySerializer()));
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, stringTemplate);
        return templates;
    }
}
