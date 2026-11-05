package com.ecommerce.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// Built by the orchestrator from its own order, so inventory never has to carry these fields
record ChargePaymentCommand(UUID commandId, String commandType, Instant issuedAt, UUID orderId, String customerId,
                            BigDecimal amount, String currency) {

    static final String TYPE = "ChargePayment";

    static ChargePaymentCommand from(Order order) {
        return new ChargePaymentCommand(UUID.randomUUID(), TYPE, Instant.now(), order.getId(), order.getCustomerId(),
                order.getTotalAmount(), order.getCurrency());
    }
}
