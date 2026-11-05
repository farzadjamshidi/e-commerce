package com.ecommerce.inventory;

import java.time.Instant;
import java.util.UUID;

// The reply to ReserveStock. No order data is forwarded: the orchestrator already has it.
record StockReservedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {

    static final String TYPE = "StockReserved";

    static StockReservedEvent from(ReserveStockCommand command) {
        return new StockReservedEvent(UUID.randomUUID(), TYPE, Instant.now(), command.orderId());
    }
}
