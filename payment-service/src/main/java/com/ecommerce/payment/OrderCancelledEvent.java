package com.ecommerce.payment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record OrderCancelledEvent(UUID orderId, String reason) {

    static final String TYPE = "OrderCancelled";
}
