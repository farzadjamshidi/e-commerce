package com.ecommerce.payment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record RefundPaymentCommand(UUID orderId) {

    static final String TYPE = "RefundPayment";
}
