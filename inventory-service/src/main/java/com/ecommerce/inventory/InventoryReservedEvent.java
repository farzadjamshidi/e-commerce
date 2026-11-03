package com.ecommerce.inventory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// Carries the order data payment needs, so payment does not have to read OrderCreated as well
record InventoryReservedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String sku,
                              int quantity, String customerId, BigDecimal totalAmount, String currency) {

    static final String TYPE = "InventoryReserved";

    static InventoryReservedEvent from(OrderCreatedEvent order) {
        return new InventoryReservedEvent(UUID.randomUUID(), TYPE, Instant.now(), order.orderId(), order.sku(),
                order.quantity(), order.customerId(), order.totalAmount(), order.currency());
    }
}
