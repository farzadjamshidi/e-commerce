package com.ecommerce.order;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

// The fields the orchestrator needs from the replies of payment
@JsonIgnoreProperties(ignoreUnknown = true)
record PaymentReply(UUID orderId, String reason) {

    static final String COMPLETED = "PaymentCompleted";
    static final String FAILED = "PaymentFailed";
    static final String REFUNDED = "PaymentRefunded";
}
