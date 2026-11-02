package com.ecommerce.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
class OrderEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    private final ProcessedEventRepository processedEventRepository;
    private final StockRepository stockRepository;
    private final JsonMapper jsonMapper;

    OrderEventHandler(ProcessedEventRepository processedEventRepository, StockRepository stockRepository,
                      JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.stockRepository = stockRepository;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row and the stock change commit together, so a redelivered event is skipped
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        // Checked before the inbox: an ignored type must not be marked processed,
        // or it would be skipped as a duplicate once this service starts handling it
        if (!OrderCreatedEvent.TYPE.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        OrderCreatedEvent event = jsonMapper.readValue(payload, OrderCreatedEvent.class);
        if (stockRepository.reserve(event.sku(), event.quantity()) == 1) {
            log.info("Reserved {} x {} for order {}", event.quantity(), event.sku(), event.orderId());
        } else {
            // Recorded as processed anyway: retrying will not create stock. The order is told in Phase 3.
            log.warn("Insufficient stock for {} x {}, order {}", event.quantity(), event.sku(), event.orderId());
        }
    }
}
