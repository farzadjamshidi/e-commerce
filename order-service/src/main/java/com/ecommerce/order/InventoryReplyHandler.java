package com.ecommerce.order;

import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class InventoryReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryReplyHandler.class);
    private static final Set<String> HANDLED = Set.of(StockReply.RESERVED, StockReply.REJECTED, StockReply.RELEASED);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final JsonMapper jsonMapper;

    InventoryReplyHandler(ProcessedEventRepository processedEventRepository,
                          OrderSagaOrchestrator orderSagaOrchestrator, JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.orderSagaOrchestrator = orderSagaOrchestrator;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row, the saga step and the next command commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        if (!HANDLED.contains(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        StockReply reply = jsonMapper.readValue(payload, StockReply.class);
        switch (eventType) {
            case StockReply.RESERVED -> orderSagaOrchestrator.onStockReserved(reply.orderId());
            case StockReply.REJECTED -> orderSagaOrchestrator.onStockRejected(reply.orderId(), reply.reason());
            default -> orderSagaOrchestrator.onStockReleased(reply.orderId());
        }
    }
}
