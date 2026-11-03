package com.ecommerce.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record PaymentFailedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId, BigDecimal amount,
                          String currency, String reason) {

    static final String TYPE = "PaymentFailed";
    static final String AMOUNT_LIMIT_EXCEEDED = "AMOUNT_LIMIT_EXCEEDED";

    static PaymentFailedEvent amountLimitExceeded(InventoryReservedEvent reserved) {
        return new PaymentFailedEvent(UUID.randomUUID(), TYPE, Instant.now(), reserved.orderId(),
                reserved.totalAmount(), reserved.currency(), AMOUNT_LIMIT_EXCEEDED);
    }
}
