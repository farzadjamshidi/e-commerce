package com.ecommerce.inventory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final long processingDelayMs;

    OrderEventListener(@Value("${inventory.processing-delay-ms:0}") long processingDelayMs) {
        this.processingDelayMs = processingDelayMs;
    }

    @KafkaListener(topics = "orders")
    void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws InterruptedException {
        Thread.sleep(processingDelayMs);
        log.info("Processed {} for {} <- partition {} offset {}",
                record.value(), record.key(), record.partition(), record.offset());
        ack.acknowledge();
    }
}
