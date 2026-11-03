package com.ecommerce.inventory;

import java.time.Instant;
import java.util.UUID;

record InventoryRejectedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String sku,
                              int quantity, String reason) {

    static final String TYPE = "InventoryRejected";
    static final String INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK";

    static InventoryRejectedEvent insufficientStock(OrderCreatedEvent order) {
        return new InventoryRejectedEvent(UUID.randomUUID(), TYPE, Instant.now(), order.orderId(), order.sku(),
                order.quantity(), INSUFFICIENT_STOCK);
    }
}
