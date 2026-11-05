package com.ecommerce.payment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record ChargePaymentCommand(UUID orderId, String customerId, BigDecimal amount, String currency) {

    static final String TYPE = "ChargePayment";
}
