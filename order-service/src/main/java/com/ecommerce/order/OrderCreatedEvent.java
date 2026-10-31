package com.ecommerce.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record OrderCreatedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID orderId,
        long orderVersion,
        String customerId,
        String sku,
        int quantity,
        BigDecimal totalAmount,
        String currency) {

    static final String TYPE = "OrderCreated";

    static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(UUID.randomUUID(), TYPE, Instant.now(), order.getId(), order.getVersion(),
                order.getCustomerId(), order.getSku(), order.getQuantity(), order.getTotalAmount(),
                order.getCurrency());
    }
}
