package com.ecommerce.inventory;

import java.time.Instant;
import java.util.UUID;

record StockReleasedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {

    static final String TYPE = "StockReleased";

    static StockReleasedEvent from(ReleaseStockCommand command) {
        return new StockReleasedEvent(UUID.randomUUID(), TYPE, Instant.now(), command.orderId());
    }
}
