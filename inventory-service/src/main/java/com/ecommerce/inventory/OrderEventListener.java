package com.ecommerce.inventory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    @KafkaListener(topics = "orders")
    void onOrderEvent(ConsumerRecord<String, String> record) {
        log.info("Received {} for {} <- partition {} offset {}",
                record.value(), record.key(), record.partition(), record.offset());
    }
}
