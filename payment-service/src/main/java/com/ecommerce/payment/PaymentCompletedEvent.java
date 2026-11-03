package com.ecommerce.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record PaymentCompletedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, BigDecimal amount,
                             String currency) {

    static final String TYPE = "PaymentCompleted";

    static PaymentCompletedEvent from(InventoryReservedEvent reserved) {
        return new PaymentCompletedEvent(UUID.randomUUID(), TYPE, Instant.now(), reserved.orderId(),
                reserved.totalAmount(), reserved.currency());
    }
}
