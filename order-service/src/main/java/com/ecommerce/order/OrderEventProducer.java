package com.ecommerce.order;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
class OrderEventProducer {

    static final String TOPIC = "orders";

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    CompletableFuture<SendResult<String, String>> send(String orderId, String event) {
        return kafkaTemplate.send(TOPIC, orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send {} for {}", event, orderId, ex);
                    } else {
                        var metadata = result.getRecordMetadata();
                        log.info("Sent {} for {} -> partition {} offset {}",
                                event, orderId, metadata.partition(), metadata.offset());
                    }
                });
    }

    void flush() {
        kafkaTemplate.flush();
    }
}
