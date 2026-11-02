package com.ecommerce.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
class OrderEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    private final ProcessedEventRepository processedEventRepository;

    OrderEventHandler(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    // The inbox row and the business change commit together, so a redelivered event is skipped
    @Transactional
    boolean handle(UUID eventId, String eventType, String orderId) {
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {} for {}, skipped", eventType, eventId, orderId);
            return false;
        }
        // Stock reservation goes here in a later step
        log.info("Handled {} {} for {}", eventType, eventId, orderId);
        return true;
    }
}
