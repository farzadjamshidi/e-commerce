package com.ecommerce.order;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class InventoryReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryReplyHandler.class);

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
        if (!StockReply.RESERVED.equals(eventType) && !StockReply.REJECTED.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        StockReply reply = jsonMapper.readValue(payload, StockReply.class);
        if (StockReply.RESERVED.equals(eventType)) {
            orderSagaOrchestrator.onStockReserved(reply.orderId());
        } else {
            orderSagaOrchestrator.onStockRejected(reply.orderId(), reply.reason());
        }
    }
}
