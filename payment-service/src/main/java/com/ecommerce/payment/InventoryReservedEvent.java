package com.ecommerce.payment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record InventoryReservedEvent(UUID orderId, String customerId, BigDecimal totalAmount, String currency) {

    static final String TYPE = "InventoryReserved";
}
