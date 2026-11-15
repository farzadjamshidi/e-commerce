package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// The end of an order, published on the orders topic next to OrderCreated. Nothing in the saga needs it:
// it is for whoever shows the order, such as the status stream to the browser.
@JsonIgnoreProperties(ignoreUnknown = true)
record OrderFinishedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, OrderStatus status) {

    static final String CONFIRMED = "OrderConfirmed";
    static final String CANCELLED = "OrderCancelled";

    static OrderFinishedEvent of(UUID orderId, OrderStatus status) {
        String type = status == OrderStatus.CONFIRMED ? CONFIRMED : CANCELLED;
        return new OrderFinishedEvent(UUID.randomUUID(), type, Instant.now(), orderId, status);
    }
}
