package com.ecommerce.inventory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
class StockCommandListener {

    private static final Logger log = LoggerFactory.getLogger(StockCommandListener.class);

    private final StockCommandHandler stockCommandHandler;
    private final long processingDelayMs;

    StockCommandListener(StockCommandHandler stockCommandHandler,
                       @Value("${inventory.processing-delay-ms:0}") long processingDelayMs) {
        this.stockCommandHandler = stockCommandHandler;
        this.processingDelayMs = processingDelayMs;
    }

    @KafkaListener(topics = "inventory-commands")
    void onStockCommand(ConsumerRecord<String, String> record, Acknowledgment ack) throws InterruptedException {
        Thread.sleep(processingDelayMs);
        String eventId = header(record, "eventId");
        if (eventId == null) {
            log.warn("No eventId header <- partition {} offset {}, skipped", record.partition(), record.offset());
        } else {
            stockCommandHandler.handle(UUID.fromString(eventId), header(record, "eventType"), record.value());
        }
        // Acknowledged only after the database commit. A crash in between means redelivery, which the inbox absorbs.
        ack.acknowledge();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
