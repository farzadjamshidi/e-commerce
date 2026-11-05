package com.ecommerce.inventory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

@Configuration
class KafkaErrorConfig {

    // Blocking retry, not retry topics: the partition waits, so ReleaseStock can never pass its ReserveStock.
    // After the last attempt the record goes to <topic>-dlt, in the same partition number.
    @Bean
    DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2);
        backOff.setMaxElapsedTime(7000);
        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaOperations), backOff);
        // A broken payload stays broken, so retrying only blocks the partition
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        // With manual acks nobody else commits the skipped record, and a restart would send it to the DLT again
        handler.setCommitRecovered(true);
        return handler;
    }
}
