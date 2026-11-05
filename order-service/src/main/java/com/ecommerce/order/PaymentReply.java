package com.ecommerce.order;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

// The fields the orchestrator needs from PaymentCompleted and PaymentFailed
@JsonIgnoreProperties(ignoreUnknown = true)
record PaymentReply(UUID orderId, String reason) {

    static final String COMPLETED = "PaymentCompleted";
    static final String FAILED = "PaymentFailed";
}
