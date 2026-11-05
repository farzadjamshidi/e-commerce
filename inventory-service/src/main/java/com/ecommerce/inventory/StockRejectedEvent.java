package com.ecommerce.inventory;

import java.time.Instant;
import java.util.UUID;

record StockRejectedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String reason) {

    static final String TYPE = "StockRejected";
    static final String INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK";

    static StockRejectedEvent insufficientStock(ReserveStockCommand command) {
        return new StockRejectedEvent(UUID.randomUUID(), TYPE, Instant.now(), command.orderId(), INSUFFICIENT_STOCK);
    }
}
