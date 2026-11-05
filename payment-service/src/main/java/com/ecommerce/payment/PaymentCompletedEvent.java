package com.ecommerce.payment;

import java.time.Instant;
import java.util.UUID;

record PaymentCompletedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {

    static final String TYPE = "PaymentCompleted";

    static PaymentCompletedEvent from(ChargePaymentCommand command) {
        return new PaymentCompletedEvent(UUID.randomUUID(), TYPE, Instant.now(), command.orderId());
    }
}
