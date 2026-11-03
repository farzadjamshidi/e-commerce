package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

record OrderCancelledEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String reason) {

    static final String TYPE = "OrderCancelled";
    static final String TIMEOUT = "TIMEOUT";

    static OrderCancelledEvent of(UUID orderId, String reason) {
        return new OrderCancelledEvent(UUID.randomUUID(), TYPE, Instant.now(), orderId, reason);
    }
}
