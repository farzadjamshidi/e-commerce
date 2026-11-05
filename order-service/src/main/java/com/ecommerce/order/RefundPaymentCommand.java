package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

record RefundPaymentCommand(UUID commandId, String commandType, Instant issuedAt, UUID orderId) {

    static final String TYPE = "RefundPayment";

    static RefundPaymentCommand of(UUID orderId) {
        return new RefundPaymentCommand(UUID.randomUUID(), TYPE, Instant.now(), orderId);
    }
}
