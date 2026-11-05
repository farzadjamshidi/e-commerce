package com.ecommerce.payment;

import java.time.Instant;
import java.util.UUID;

record PaymentRefundedEvent(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {

    static final String TYPE = "PaymentRefunded";

    static PaymentRefundedEvent from(RefundPaymentCommand command) {
        return new PaymentRefundedEvent(UUID.randomUUID(), TYPE, Instant.now(), command.orderId());
    }
}
